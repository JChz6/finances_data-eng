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



--DDL Gasto acumulado diariamente VS presupuesto POR CATEGORÍA
CREATE OR REPLACE VIEW `big-query-406221.finanzas_personales.agregado` AS(
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
