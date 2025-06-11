--DDL HISTORICO
CREATE TABLE `big-query-406221.finanzas_personales.historico` (
    fecha DATE,
    cuenta STRING,
    categoria STRING,
    subcategoria STRING,
    nota STRING,
    ingreso_gasto STRING,
    importe FLOAT64,
    moneda STRING,
    comentario STRING,
    fecha_carga DATETIME,
    dias_trabajados FLOAT64
)
PARTITION BY fecha;



--DDL EMOCIONAL
CREATE TABLE `big-query-406221.finanzas_personales.emocional` (
    fecha DATE,
    cuenta STRING,
    categoria STRING,
    subcategoria STRING,
    nota STRING,
    ingreso_gasto STRING,
    importe FLOAT64,
    moneda STRING,
    comentario STRING,
    fecha_carga DATETIME,
    clave STRING,
    valor STRING
)
PARTITION BY DATE_TRUNC(fecha, YEAR);


--DDL PRESUPUESTOS MENSUALES
CREATE TABLE `big-query-406221.finanzas_personales.presupuesto`
(
    fecha DATE,
    categoria STRING,
    presupuesto FLOAT64
)
PARTITION BY (fecha)
CLUSTER BY categoria;


--DDL Porcentaje mensual de gasto en mujeres VS ingreso mensual total
CREATE OR REPLACE VIEW `big-query-406221.finanzas_personales.mujeres_agg` AS(
SELECT
DATE_TRUNC(fecha, month) as fecha_trunc,
EXTRACT(YEAR FROM fecha) as anio,
EXTRACT(MONTH FROM fecha) as mes,
ROUND((SUM(CASE WHEN categoria != "Cuotas de terceros" AND ingreso_gasto = "Ingreso" THEN importe ELSE 0 END)), 2) as ingreso_total,
ROUND(SUM(CASE WHEN ingreso_gasto = 'Gastos' AND categoria != 'Préstamos' AND subcategoria = 'Mujeres' THEN importe ELSE 0 END), 3) as gasto_mujeres,
ROUND(SUM(CASE WHEN ingreso_gasto = 'Gastos' AND categoria != 'Préstamos' AND subcategoria = 'Mujeres' THEN importe ELSE 0 END) /
    (SUM(CASE WHEN categoria != "Cuotas de terceros" AND ingreso_gasto = "Ingreso" THEN importe ELSE 0 END)) * 100, 2) || "%"
     as porc_mujeres
FROM `big-query-406221.finanzas_personales.historico`
GROUP BY 
fecha_trunc, EXTRACT(YEAR FROM fecha), EXTRACT(MONTH FROM fecha)
ORDER BY anio DESC, mes DESC
)
;




--DDL Gasto AGREGADO diariamente VS presupuesto POR CATEGORÍA
CREATE OR REPLACE VIEW `big-query-406221.finanzas_personales.agregado` AS(
SELECT
  DATE_TRUNC(h.fecha, MONTH) AS fecha, h.categoria,
  SUM(h.importe) OVER(PARTITION BY EXTRACT(YEAR FROM h.fecha) , EXTRACT(MONTH FROM h.fecha), h.categoria) as gasto_acumulado_mes,
  p.presupuesto,
FROM `big-query-406221.finanzas_personales.historico` h
LEFT JOIN `big-query-406221.finanzas_personales.presupuesto` p
 ON UPPER(h.categoria) = UPPER(p.categoria) AND
    CAST(p.month AS INT64) = EXTRACT(MONTH FROM h.fecha) AND
    CAST(p.year AS INT64) = EXTRACT(YEAR FROM h.fecha)
 WHERE h.ingreso_gasto =  "Gastos"
 QUALIFY ROW_NUMBER() OVER(PARTITION BY EXTRACT(MONTH FROM h.fecha), categoria) = 1
 ORDER BY DATE_TRUNC(h.fecha, MONTH) DESC, h.categoria
);




--DDL Gasto acumulado diariamente VS presupuesto POR CATEGORÍA
CREATE OR REPLACE VIEW `big-query-406221.finanzas_personales.acumulado` AS(
SELECT DISTINCT
  h.fecha, h.categoria,
  SUM(h.importe) OVER(PARTITION BY h.categoria, h.ingreso_gasto, EXTRACT (MONTH FROM h.fecha) ORDER BY h.fecha ASC) as gasto_acumulado_mes,
  p.presupuesto,
  ROUND((SUM(h.importe) OVER(PARTITION BY h.categoria, h.ingreso_gasto, EXTRACT (MONTH FROM h.fecha) ORDER BY h.fecha ASC)) / p.presupuesto, 3) as porc_consumido,
  ROUND((SUM(h.importe) OVER(PARTITION BY h.categoria, h.ingreso_gasto, EXTRACT (MONTH FROM h.fecha) ORDER BY h.fecha ASC)) / p.presupuesto *100, 2)||"%" as porc_consumido_str
  FROM `big-query-406221.finanzas_personales.historico` h
 JOIN `big-query-406221.finanzas_personales.presupuesto` p
 ON UPPER(h.categoria) = UPPER(p.categoria) AND
    CAST(p.month AS INT64) = EXTRACT(MONTH FROM h.fecha) AND
    CAST(p.year AS INT64) = EXTRACT(YEAR FROM h.fecha)
 WHERE h.ingreso_gasto =  "Gastos"
 ORDER BY h.fecha DESC, h.categoria
);


--DDL pago_por_hora
CREATE OR REPLACE VIEW `big-query-406221.finanzas_personales.pago_por_hora` AS(
  WITH salario_mensual AS (
  SELECT
    fecha,
    EXTRACT(YEAR FROM fecha) as anio,
    EXTRACT(MONTH FROM fecha) as mes,
    categoria,
    nota,
    importe,
    SUM(
      CASE
        WHEN categoria = 'Salario' THEN importe
        ELSE 0
      END
    ) OVER(PARTITION BY EXTRACT(YEAR FROM fecha), EXTRACT(MONTH FROM fecha)) as bruto_mensual,
    SUM(
      CASE
        WHEN categoria != 'Salario' THEN importe
        ELSE 0
      END
    ) OVER(PARTITION BY EXTRACT(YEAR FROM fecha), EXTRACT(MONTH FROM fecha)) as descuentos,
    SUM (dias_trabajados) OVER(PARTITION BY EXTRACT(YEAR FROM fecha), EXTRACT(MONTH FROM fecha)) as trabajo_mensual,
    dias_trabajados,
    fecha_carga
  FROM `big-query-406221.finanzas_personales.historico`
  WHERE categoria = 'Salario'
  OR nota IN ('Aporte AFP', 'Seguro AFP', 'Quinta categoría')
  )
  SELECT fecha, anio, mes, bruto_mensual, descuentos,
  bruto_mensual - descuentos as neto_mensual,
  trabajo_mensual,
  ROUND(trabajo_mensual * 0.723, 2) as trabajo_neto,
  (ROUND(trabajo_mensual * 0.723, 2)) * 8 as horas_trabajadas,
  ROUND((bruto_mensual - descuentos) / ((ROUND(trabajo_mensual * 0.723, 2)) * 8),2) as pago_por_hora,
  fecha_carga 
  FROM salario_mensual
  QUALIFY ROW_NUMBER() OVER(PARTITION BY anio, mes ORDER BY fecha DESC) = 1
  ORDER BY anio DESC, mes DESC
);


--DDL PARA COSTO EN VIDA
CREATE OR REPLACE VIEW `big-query-406221.finanzas_personales.costo_en_vida` AS(
  WITH ultimo_pago AS (
    SELECT
      horas_trabajadas,
      pago_por_hora
    FROM
      `big-query-406221.finanzas_personales.pago_por_hora`
    ORDER BY
      anio DESC, mes DESC
    LIMIT 1
  )
  SELECT
    h.fecha,
    EXTRACT(YEAR FROM h.fecha) as anio,
    EXTRACT(MONTH FROM h.fecha) as mes,
    h.cuenta,
    h.categoria,
    h.subcategoria,
    h.nota,
    h.comentario,
    h.importe,
    COALESCE(v.horas_trabajadas, u.horas_trabajadas) AS horas_trabajadas,
    COALESCE(v.pago_por_hora, u.pago_por_hora) AS pago_por_hora,
    ROUND((h.importe / COALESCE(v.pago_por_hora, u.pago_por_hora)), 3) AS costo_en_horas,
    h.moneda,
    h.fecha_carga
  FROM
    `big-query-406221.finanzas_personales.historico` h
  LEFT JOIN
    `big-query-406221.finanzas_personales.pago_por_hora` v
  ON
    EXTRACT(YEAR FROM h.fecha) = v.anio AND EXTRACT(MONTH FROM h.fecha) = v.mes
  CROSS JOIN
    ultimo_pago u
  WHERE
    h.ingreso_gasto = 'Gastos'
  ORDER BY
    h.fecha DESC
);





--DDL TABLA EXTERNA DE PRESUPUESTOS DE METAS
CREATE OR REPLACE EXTERNAL TABLE `big-query-406221.finanzas_personales.presupuesto_independencia` (
  categoria STRING,
  subcategoria STRING,
  articulo STRING,
  importe NUMERIC,
  importe_real NUMERIC,
  nota STRING,
  recurrente BOOLEAN,
  frecuencia_anual STRING,
  indispensable BOOLEAN,
  necesario_alquiler BOOLEAN
)
OPTIONS (
  format = 'GOOGLE_SHEETS',
  uris = ['https://docs.google.com/spreadsheets/d/13EaVzRlloe5QM9OuazZJpMU_Uk5y0FPpht52YjYPdkk'],
  skip_leading_rows = 4
);



--DDL NUEVA TABLA EXTERNA DE PRESUPUESTOS MENSUALES
CREATE OR REPLACE EXTERNAL TABLE `big-query-406221.finanzas_personales.presupuesto` (
  fecha DATE,
  year STRING,
  month STRING,
  categoria STRING,
  presupuesto FLOAT64
)
OPTIONS (
  format = 'GOOGLE_SHEETS',
  uris = ['https://docs.google.com/spreadsheets/d/1s3m_ZWgQ7W0PIwY7W-h40ZWob9_5dmycN8baDO1OxBs'],
  skip_leading_rows = 1
);


--DDL TABLA DE CLAVES
CREATE OR REPLACE EXTERNAL TABLE `big-query-406221.finanzas_personales.claves` (
  numero STRING,
  nombre STRING,
  clave STRING,
  posicion STRING,
  descripcion STRING,
  funcion_pandas STRING,
  fecha_implementacion STRING
)
OPTIONS (
  format = 'GOOGLE_SHEETS',
  uris = ['https://docs.google.com/spreadsheets/d/18Ccfa_hadRcp1gILp_1WtCAJbgrU_oHmpVAWCbQ-ODs'],
  skip_leading_rows = 1
);




--DDL Resumen y promedio anual de ingresos (FREELANCE Y SALARIO)
CREATE OR REPLACE VIEW `big-query-406221.finanzas_personales.ingresos_resumen` AS(
  WITH ingresos_mensuales AS (
    SELECT
      EXTRACT(YEAR FROM fecha) AS anio,
      EXTRACT(MONTH FROM fecha) AS mes,
      SUM(CASE WHEN categoria IN ('Salario', 'Freelance', 'Negocios') THEN importe ELSE 0 END) AS ingreso_bruto,
      SUM(CASE WHEN categoria IN ('Impuestos', 'Jubilación', 'Inversiones') THEN importe ELSE 0 END) AS descuentos
    FROM `big-query-406221.finanzas_personales.historico`
    GROUP BY anio, mes
  )
  SELECT
    DATE(anio, mes, 1) as fecha,
    anio,
    mes,
    ingreso_bruto,
    descuentos,
    ingreso_bruto - descuentos AS ingreso_neto,
    ROUND(AVG(ingreso_bruto - descuentos) OVER(PARTITION BY anio), 2) AS ingreso_neto_promedio
  FROM ingresos_mensuales
  ORDER BY anio desc , mes desc
)
    
