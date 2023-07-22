create table clima (
    fecha TIMESTAMP primary key not null,
    temperatura DECIMAL not null,
    precipitacion INTEGER not null,
    lluvia DECIMAL not null,
    visibilidad DECIMAL not null,
    velocidadViento DECIMAL not null
);