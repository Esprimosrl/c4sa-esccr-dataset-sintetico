COPY(
SELECT
  civ_ann.*,civ_zona.prg_codice_zona_new as zona_costruzione
FROM
  read_csv_auto('./dataset/andria_distribuzione_annuale_zone_inferite.csv') as civ_ann
  LEFT JOIN read_csv_auto('./civici_con_zona_costruzione_associata.csv') as civ_zona ON civ_ann.id_civico = civ_zona.PROGRESSIVO_ACCESSO
) TO './andria_distribuzione_annuale_zone_inferite_e_zone_costruzione.csv' (HEADER, DELIMITER ',')
