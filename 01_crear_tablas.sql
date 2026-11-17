-- 1. Tabla Dimensión Tiempo
CREATE TABLE dim_tiempo (
    id_tiempo SERIAL PRIMARY KEY,
    anio INT NOT NULL,
    mes INT NOT NULL,
    nombre_mes VARCHAR(20) NOT NULL,
    trimestre VARCHAR(10) NOT NULL,
    CONSTRAINT uq_tiempo UNIQUE (anio, mes)
);

-- 2. Tabla Dimensión Ubicación Geográfica
CREATE TABLE dim_ubicacion (
    id_ubicacion SERIAL PRIMARY KEY,
    ccdd INT NOT NULL,
    departamento VARCHAR(100) NOT NULL,
    provincia VARCHAR(100) NOT NULL,
    distrito VARCHAR(100) NOT NULL,
    CONSTRAINT uq_ubicacion UNIQUE (ccdd, departamento, provincia, distrito)
);

-- 3. Tabla Dimensión Comisaría / Dependencia Policial
CREATE TABLE dim_comisaria (
    id_comisaria SERIAL PRIMARY KEY,
    nombre_comisaria VARCHAR(150) NOT NULL UNIQUE
);

-- 4. Tabla Dimensión Tipificación del Delito
CREATE TABLE dim_tipificacion (
    id_tipificacion SERIAL PRIMARY KEY,
    categoria VARCHAR(100) NOT NULL,
    codigo_comision VARCHAR(100) NOT NULL,
    descripcion_detalle VARCHAR(150) NOT NULL,
    CONSTRAINT uq_tipificacion UNIQUE (categoria, codigo_comision)
);

-- 5. Tabla de Hechos: Denuncias por Trata de Personas
CREATE TABLE hechos_denuncias (
    id_hecho SERIAL PRIMARY KEY,
    id_tiempo INT NOT NULL REFERENCES dim_tiempo(id_tiempo),
    id_ubicacion INT NOT NULL REFERENCES dim_ubicacion(id_ubicacion),
    id_comisaria INT NOT NULL REFERENCES dim_comisaria(id_comisaria),
    id_tipificacion INT NOT NULL REFERENCES dim_tipificacion(id_tipificacion),
    valor INT NOT NULL DEFAULT 0
);

-- Índices para acelerar los JOINs en Spark SQL
CREATE INDEX idx_hechos_tiempo ON hechos_denuncias(id_tiempo);
CREATE INDEX idx_hechos_ubicacion ON hechos_denuncias(id_ubicacion);
CREATE INDEX idx_hechos_comisaria ON hechos_denuncias(id_comisaria);
CREATE INDEX idx_hechos_tipificacion ON hechos_denuncias(id_tipificacion);