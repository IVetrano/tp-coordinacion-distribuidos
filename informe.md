# Informe del Trabajo Práctico
- Alumno: Ignacio Ezequiel Vetrano
- Padrón: 10129  

Este informe explica las decisiones tomadas para el escalamiento de la cantidad de clientes y controles del proyecto, así como la coordinación de los mismos

## Escalamiento de la cantidad de clientes
Para lograr que el sistema pueda resolver multiples consultas de manera concurrente, se identificó cada conexión con un `client_id` único generado, este es un `uuid` generado por el `MessageHandler`, aprovechando que el gateway instancia un `MessageHandler` por cada conexión, cada instancia de `MessageHandler` tiene su propio `client_id` y lo propaga en todos los mensajes internos.  

De esta forma, aunque varios clientes envien datos en paralelo, cada servicio procesa y almacena informacion separada por client_id. Esto permite que el sistema escale en cantidad de clientes sin cambiar la logica principal de negocio.

## Coordinación de las instancias de Sum
Las distintas instancias de Sum van a ir recibiendo frutas de distintos clientes y guardando el conteo de las mismas separadas por el `client_id`.

Varias instancias de Sum van a recibir por la `input_queue` frutas del mismo cliente, pero solo una de ellas recibirá por ella el `EOF` de ese cliente. Que lo haya recibido significa que todas las frutas del cliente fueron enviadas por el gateway, pero no necesariamente que todas fueron procesadas por las instancias de Sum, ya que pueden haber mensajes en vuelo en el hilo de datos de alguna instancia.

Para resolver esto, el `MessageHandler` del gateway incluye en el mensaje de `EOF` el total de mensajes de datos enviados para ese cliente (`total_messages`). La instancia que recibe el `EOF` se convierte en coordinadora y utiliza este valor para verificar que todos los datos fueron efectivamente procesados antes de ordenar el flush.

La coordinación se implementa mediante dos hilos por instancia: uno consume la `input_queue` (datos y EOF del gateway) y otro consume un exchange `direct` llamado `SUM_CONTROL_EXCHANGE`, con una routing key dedicada por instancia (`SUM_CONTROL_EXCHANGE_{ID}`). El protocolo de coordinación entre instancias consta de los siguientes mensajes:

- `AMOUNT_REQUEST`: el coordinador lo broadcastea a todas las instancias preguntando cuántos mensajes procesaron para ese cliente
- `AMOUNT_RESPONSE`: cada instancia responde al coordinador con su `messages_received`
- `FLUSH_MESSAGE`: el coordinador lo broadcastea cuando verificó que la suma de todos los `messages_received` coincide con `total_messages`, indicando que es seguro hacer flush
- `MAX_AMOUNT_REACHED_MESSAGE`: el coordinador lo broadcastea si se superaron los `MAX_ATTEMPTS` reintentos sin lograr que los counts coincidan

El flujo completo es el siguiente: al recibir el `EOF`, el coordinador broadcastea un `AMOUNT_REQUEST` a todas las instancias incluyendo a sí mismo. Cada instancia responde con su `messages_received`. Cuando el coordinador recibe todas las respuestas compara la suma con `total_messages`:

- Si coinciden: broadcastea `FLUSH_MESSAGE` y cada instancia envía sus datos acumulados a los Aggregators seguido de un `EOF`
- Si no coinciden: hay datos todavía en vuelo en algún hilo de datos, por lo que reintenta el `AMOUNT_REQUEST`. Eventualmente las instancias que faltaban terminarán de procesar los datos e incrementarán su `messages_received`, logrando de esta manera que reintentando el `AMOUNT_REQUEST` la suma coincida
- Si se superan los `MAX_ATTEMPTS`: broadcastea `MAX_AMOUNT_REACHED_MESSAGE`, cada instancia descarta su estado para ese cliente y envía únicamente el `EOF` a los Aggregators sin datos, permitiendo que el pipeline continúe sin residuos. Estos clientes recibirán un top vacío.  

`MAX_ATTEMPTS` en una version final sería una cantidad configurable, la cual actualmente quedó como constante porque no podemos modificar el `docker-compose.yml` para agregar mas environment variables.

Si bien Python tiene el `GIL (Global Interpreter Lock)` que impide la ejecución simultánea de dos hilos, esto no representa un problema en este caso. Ambos hilos pasan la mayor parte de su tiempo bloqueados esperando mensajes de RabbitMQ, que es una operación de I/O. Durante estas esperas el GIL es liberado, permitiendo que el otro hilo avance. En la práctica ambos hilos corren de manera efectivamente concurrente ya que el cuello de botella es la red y no el procesador.

## Coordinación de las instancias de Aggregation
Cada instancia de Sum rutea cada fruta a un único Aggregator determinado por un `hash` de la fruta (se optó por `MD5` debido a que provee una distribución uniforme y determinística entre instancias, evitando el costo computacional innecesario de funciones criptográficas más robustas como SHA-256). De esta forma cada Aggregator recibe un subconjunto disjunto de frutas, eliminando procesamiento redundante.

Internamente cada Aggregator tiene identificados los tops parciales actuales de cada cliente por el `client_id`.

Para que cada Aggregator sepa cuando todas las instancias de Sum terminaron de enviar sus datos, cada instancia de Sum envia un `EOF` al terminar el flush. El Aggregator espera recibir `SUM_AMOUNT` mensajes de `EOF` antes de calcular su top parcial y enviarlo al Join.

Se decidió cambiar la estructura interna del `fruit_top` de lista a diccionario, acumulando los valores durante `_process_data` y ordenando recién al recibir el `EOF`. Esto simplifica el procesamiento de cada mensaje individual a costa de un pico de procesamiento en el `_process_eof`, que se consideró aceptable dado que ocurre una sola vez por cliente.

## Coordinación del Joiner
El Join recibe un top parcial de cada instancia de Aggregator, identificados por `client_id`. Por cada cliente lleva la cuenta de cuántos tops parciales recibió y mantiene un top acumulado que se va fusionando incrementalmente: al llegar cada top parcial se combina con el acumulado, se ordena y se recorta a `TOP_SIZE` elementos, evitando tener que guardar todos los tops en memoria hasta el final.

Una vez recibidos los `AGGREGATION_AMOUNT` tops parciales de un cliente, el Join envía el top final consolidado al gateway con el `client_id` correspondiente para que sea entregado al cliente correcto, y libera el estado asociado a ese cliente.