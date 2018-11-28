## Load testing

### Stage-2 PUT
 
Linear - https://overload.yandex.net/139744

Linear + Const - https://overload.yandex.net/139749

### Stage-2 GET
 
Linear - https://overload.yandex.net/139752

Linear + Const - https://overload.yandex.net/139753

### Stage-2 MIX

Linear - https://overload.yandex.net/139755

Linear + Const - https://overload.yandex.net/139758


### Stage-3 PUT

Linear - https://overload.yandex.net/136934

Linear + Const - https://overload.yandex.net/139082

### Stage-3 GET

Linear - https://overload.yandex.net/136949

Linear + Const - https://overload.yandex.net/139083

### Stage-3 MIX
 
Linear - https://overload.yandex.net/136951

Linear + Const - https://overload.yandex.net/139084

## Profiling
JFR - https://drive.google.com/open?id=1oXQDoA-poT8FXRjHYv2aMZxuCAVylcBl
### PUT
#### before
![put_contention_before](https://raw.githubusercontent.com/gorbatovea/2018-highload-kv/Stage-3/pictures/put/before.png)
#### after

![put_contention_before](https://raw.githubusercontent.com/gorbatovea/2018-highload-kv/Stage-3/pictures/put/after.png)

Удалось существенно улучшить производительность PUT операций за счет оптимизаций записи в хранилище. 
Переход на одну IO операцию(вместо большого кол-ва IO записей в момент снятия дампа состояния) позволил очень сильно сократить contention

### GET

#### before

![get_contention_before](https://raw.githubusercontent.com/gorbatovea/2018-highload-kv/Stage-3/pictures/get/before_contention.png)
![get_method_before](https://raw.githubusercontent.com/gorbatovea/2018-highload-kv/Stage-3/pictures/get/before_get_method.png)

#### after

![get_method_before](https://raw.githubusercontent.com/gorbatovea/2018-highload-kv/Stage-3/pictures/get/after_get_method.png)


Производительность GET возросла на порядок. 
Это стало возможным благодаря уменьшению количества блокировок потоков практически до 0(после оптимизаций не удалось зафиксировать contention).
Также теперь используется mmap для чтения из дампа - это сказалось крайне положительно на производительности IO операций.
Как результат - уменьшение contention'a и рост производительности. 
