## Load testing

### Stage-2 PUT
 
Linear - https://overload.yandex.net/136917

Const - https://overload.yandex.net/139012

### Stage-2 GET
 
Linear - https://overload.yandex.net/136925

Const - https://overload.yandex.net/139018

### Stage-2 MIX

Linear - https://overload.yandex.net/136930

Const - https://overload.yandex.net/139019


### Stage-3 PUT

Linear - https://overload.yandex.net/136934

Const - https://overload.yandex.net/139026

### Stage-3 GET

Linear - https://overload.yandex.net/136949

Const - https://overload.yandex.net/139028

### Stage-3 MIX
 
Linear - https://overload.yandex.net/136951

Const - https://overload.yandex.net/139031

## Profiling
JFR - https://drive.google.com/open?id=1oXQDoA-poT8FXRjHYv2aMZxuCAVylcBl

![cat](https://avatars.mds.yandex.net/get-pdb/27625/14057772-8246-43f3-a075-5fd66c96c00a/s1200)

## Summary

### PUT

Удалось существенно улучшить производительность PUT операций за счет оптимизаций записи в хранилище. 
Переход на одну IO операцию(вместо большого кол-ва IO записей в момент снятия дампа состояния) также позволил сократить contention.

### GET

Удалось очень сильно улучшить производительность GET. 
Это стало возможным благодаря уменьшению количества блокировок потоков.
Также теперь используется mmap для чтения из дампа - это сказалось крайне положительно на производительности IO операций.
Как результат - уменьшение contention'a и рост производительности. 
