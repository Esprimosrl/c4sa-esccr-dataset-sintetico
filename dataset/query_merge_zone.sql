COPY(WITH rif AS (
             SELECT *
             FROM read_csv_auto('./andria_distribuzione_annuale.csv')
         ),
         strad AS (
             SELECT *
             FROM read_csv_auto('../data_csv_parquet/andria_stradario_zone.csv')
         ),
         strad_one AS ( -- prendiamo solo una delle possibili zone associate alla strada
             SELECT *
             FROM strad
             QUALIFY row_number() OVER (
                 PARTITION BY strada
                 ORDER BY strada
             ) = 1
         )
         SELECT rif.*, strad_one.zona
         FROM rif
         LEFT JOIN strad_one
             ON rif.odonimo = strad_one.strada) TO './andria_distribuzione_annuale_zone.csv' (HEADER, DELIMITER ',');
