# Informe del Trabajo Práctico
Este informe explica las decisiones tomadas para el escalamiento de la cantidad de clientes y controles del proyecto, así como la coordinación de los mismos

## Escalamiento de la cantidad de clientes
Para lograr que el sistema pueda resolver multiples consultas de manera concurrente, se identificó cada conexión con un `client_id` único generado, este es un `uuid` generado por el `MessageHandler`, aprovechando que el gateway instancia un `MessageHandler` por cada conexión, cada instancia de `MessageHandler` tiene su propio `client_id` y lo propaga en todos los mensajes internos.  

De esta forma, aunque varios clientes envien datos en paralelo, cada servicio procesa y almacena informacion separada por client_id. Esto permite que el sistema escale en cantidad de clientes sin cambiar la logica principal de negocio.

## Coordinación de las instancias de Sum
Las distintas instancias de Sum van a ir recibiendo frutas de distintos clientes y guardando el conteo de las mismas separadas por el `client_id`  

Varias instancias de Sum van a recibir por la `input_queue` frutas del mismo cliente, pero solo una de ellas recibirá por ella el `EOF` de ese cliente. Que lo haya recibido significa que todas las frutas del cliente fueron recibidas por una o mas instancias de Sum, por lo que la instancia que recibio el `EOF` se encargará de notificar a todas las demas instancias para que hagan flush de sus sumas a los aggregators.  

La manera en la que se resolvio la notificación es separando el flujo de datos del flujo de control, creando otro hilo que este consumiendo concurrentemente un exchange `SUM_CONTROL_EXCHANGE`. Este exchange es de tipo `direct` con una routing key dedicada por instancia (`SUM_CONTROL_EXCHANGE_{ID}`), lo que permite dirigir mensajes a una instancia específica de Sum.  

Cuando una instancia recibe el `EOF` de un cliente por la `input_queue`, se 
convierte en coordinadora para ese cliente: flushea sus propios datos acumulados 
a los Aggregators y luego envía un mensaje de control a cada una de las otras 
instancias por sus routing keys dedicadas, notificándoles que deben hacer flush de sus datos para ese cliente.  

Cada instancia, al recibir este mensaje por su hilo de control, flushea sus propios datos acumulados. De esta manera, todas las instancias terminan enviando sus parciales a los Aggregators sin necesidad de que el gateway envíe múltiples EOFs.  

Si bien Python tiene el `GIL (Global Interpreter Lock)` que impide la ejecución simultánea de dos hilos, esto no representa un problema en este caso. Ambos hilos pasan la mayor parte de su tiempo bloqueados esperando mensajes de RabbitMQ, que es una operación de I/O. Durante estas esperas el GIL es liberado, permitiendo que el otro hilo avance. En la práctica ambos hilos corren de manera efectivamente concurrente ya que el cuello de botella es la red y no el procesador.  

## Coordinación de las instancias de Aggregation
Cada instancia de Sum hace broadcast de sus datos a todos los Aggregators, pero, para evitar procesamiento redundante, cada fruta se envia a un único Aggregator determinado por un `hash` (Se optó por utilizar `MD5` como función de hash debido a que provee una distribución uniforme y determinística entre instancias, evitando el costo computacional innecesario de funciones criptográficas más robustas como SHA-256). De esta forma cada Aggregator recibe un subconjunto distinto de las frutas.  

Internamente cada Aggregator tiene identificados los tops parciales actuales de cada cliente por el `client_id`.  

Para que cada Aggregator sepa cuando todas las instancias de Sum terminaron de enviar sus datos, cada instancia de Sum envia un `EOF` al terminar el flush. El Aggregator espera recibir `SUM_AMOUNT` mensajes de `EOF` antes de calcular su top parcial y enviarlo al Join.  

Se decidió cambiar la estructura interna del `fruit_top` de lista a diccionario, acumulando los valores durante `_process_data` y ordenando recién al recibir el `EOF`. Esto simplifica el procesamiento de cada mensaje individual a costa de un pico de procesamiento en el `_process_eof`, que se consideró aceptable dado que ocurre una sola vez por cliente.

## Coordinación del Joiner
El Join recibe un top parcial de cada instancia de Aggregator, identificados por `client_id`. Por cada cliente lleva la cuenta de cuantos tops parciales recibio y mantiene un top acumulado que se va fusionando con el anterior, al llegar cada top parcial:
- Se combina con el anterior
- Se ordena
- Se recorta a `TOP_SIZE` elementos
Esto evita tener que guardar todos los tops parciales en memoria hasta el final.  

Una vez recibidos los `AGGREGATION_AMOUNT` tops parciales de un cliente, el Join envia el top final consolidado al gateway con el `client_id` correspondiente para que sea entregado al cliente correcto, y liberea el estado asociado a ese cliente.
