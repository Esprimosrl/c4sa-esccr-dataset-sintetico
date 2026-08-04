COPY(WITH my_table AS (
             SELECT
                 row_number() OVER () AS rid,
                 *,
                 NULLIF(zona, NULL) AS zona_clean
             FROM read_csv_auto('./andria_distribuzione_annuale_zone.csv')
         ),
         centroids AS (
             SELECT
                 zona_clean AS zona,
                 avg(coord_x) AS cx,
                 avg(coord_y) AS cy
             FROM my_table
             WHERE zona_clean IS NOT NULL
             GROUP BY zona_clean
         ),
         nearest AS (
             SELECT
                 t.rid,
                 c.zona AS inferred_zona,
                 sqrt(
                     pow(t.coord_x - c.cx, 2) +
                     pow(t.coord_y - c.cy, 2)
                 ) AS distance_to_centroid
             FROM my_table t
             CROSS JOIN centroids c
             WHERE t.zona_clean IS NULL
             QUALIFY row_number() OVER (
                 PARTITION BY t.rid
                 ORDER BY distance_to_centroid
             ) = 1
         )
         SELECT
             t.anno,
             t.id_civico,
             t.odonimo,
             t.localita,
             t.civico,
             t.esponente,
             t.coord_x,
             t.coord_y,
             t.densita_locale,
             t.altro_rd_t,
             t.ingombranti_misti_t,
             t.carta_cartone_t,
             t.frazione_organica_t,
             t.legno_t,
             t.metallo_t,
             t.plastica_t,
             t.raee_t,
             t.selettiva_t,
             t.tessili_t,
             t.vetro_t,
             t.rifiuti_cd_t,
             t.pulizia_stradale_recupero_t,
             t.totale_t,
             coalesce(t.zona_clean, n.inferred_zona) AS zona,
             --n.inferred_zona,
             --n.distance_to_centroid
         FROM my_table t
         LEFT JOIN nearest n USING (rid)) TO './andria_distribuzione_annuale_zone_inferite.csv' (HEADER,DELIMITER ',');
