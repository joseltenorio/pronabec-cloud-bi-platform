-- ============================================================================
-- Project Cloud BI Platform
-- ML regional mapping dimension
-- ============================================================================

CREATE OR REPLACE VIEW `{project_id}.{ml_dataset}.dim_region_mapping` AS
SELECT * FROM UNNEST([
  STRUCT('AMAZONAS' AS source_region, 'AMAZONAS' AS region_canonical, 'department' AS region_scope, 'identity' AS mapping_rule, FALSE AS is_aggregated_region, 'Canonical department region.' AS notes),
  STRUCT('ANCASH' AS source_region, 'ANCASH' AS region_canonical, 'department' AS region_scope, 'identity' AS mapping_rule, FALSE AS is_aggregated_region, 'Canonical department region; accents are normalized away.' AS notes),
  STRUCT('APURIMAC' AS source_region, 'APURIMAC' AS region_canonical, 'department' AS region_scope, 'identity' AS mapping_rule, FALSE AS is_aggregated_region, 'Canonical department region; accents are normalized away.' AS notes),
  STRUCT('AREQUIPA' AS source_region, 'AREQUIPA' AS region_canonical, 'department' AS region_scope, 'identity' AS mapping_rule, FALSE AS is_aggregated_region, 'Canonical department region.' AS notes),
  STRUCT('AYACUCHO' AS source_region, 'AYACUCHO' AS region_canonical, 'department' AS region_scope, 'identity' AS mapping_rule, FALSE AS is_aggregated_region, 'Canonical department region.' AS notes),
  STRUCT('CAJAMARCA' AS source_region, 'CAJAMARCA' AS region_canonical, 'department' AS region_scope, 'identity' AS mapping_rule, FALSE AS is_aggregated_region, 'Canonical department region.' AS notes),
  STRUCT('CUSCO' AS source_region, 'CUSCO' AS region_canonical, 'department' AS region_scope, 'identity' AS mapping_rule, FALSE AS is_aggregated_region, 'Canonical department region.' AS notes),
  STRUCT('HUANCAVELICA' AS source_region, 'HUANCAVELICA' AS region_canonical, 'department' AS region_scope, 'identity' AS mapping_rule, FALSE AS is_aggregated_region, 'Canonical department region.' AS notes),
  STRUCT('HUANUCO' AS source_region, 'HUANUCO' AS region_canonical, 'department' AS region_scope, 'identity' AS mapping_rule, FALSE AS is_aggregated_region, 'Canonical department region; accents are normalized away.' AS notes),
  STRUCT('ICA' AS source_region, 'ICA' AS region_canonical, 'department' AS region_scope, 'identity' AS mapping_rule, FALSE AS is_aggregated_region, 'Canonical department region.' AS notes),
  STRUCT('JUNIN' AS source_region, 'JUNIN' AS region_canonical, 'department' AS region_scope, 'identity' AS mapping_rule, FALSE AS is_aggregated_region, 'Canonical department region; accents are normalized away.' AS notes),
  STRUCT('LA LIBERTAD' AS source_region, 'LA LIBERTAD' AS region_canonical, 'department' AS region_scope, 'identity' AS mapping_rule, FALSE AS is_aggregated_region, 'Canonical department region.' AS notes),
  STRUCT('LAMBAYEQUE' AS source_region, 'LAMBAYEQUE' AS region_canonical, 'department' AS region_scope, 'identity' AS mapping_rule, FALSE AS is_aggregated_region, 'Canonical department region.' AS notes),
  STRUCT('LIMA' AS source_region, 'LIMA' AS region_canonical, 'department' AS region_scope, 'manual_lima_aggregation' AS mapping_rule, TRUE AS is_aggregated_region, 'Canonical Lima bucket used for regional predictive foundation.' AS notes),
  STRUCT('LIMA METROPOLITANA' AS source_region, 'LIMA' AS region_canonical, 'department' AS region_scope, 'manual_lima_aggregation' AS mapping_rule, TRUE AS is_aggregated_region, 'Mapped to Lima for regional predictive foundation; Lima subregions are not modeled separately in v1.' AS notes),
  STRUCT('LIMA PROVINCIAS' AS source_region, 'LIMA' AS region_canonical, 'department' AS region_scope, 'manual_lima_aggregation' AS mapping_rule, TRUE AS is_aggregated_region, 'Mapped to Lima for regional predictive foundation; Lima subregions are not modeled separately in v1.' AS notes),
  STRUCT('LORETO' AS source_region, 'LORETO' AS region_canonical, 'department' AS region_scope, 'identity' AS mapping_rule, FALSE AS is_aggregated_region, 'Canonical department region.' AS notes),
  STRUCT('MADRE DE DIOS' AS source_region, 'MADRE DE DIOS' AS region_canonical, 'department' AS region_scope, 'identity' AS mapping_rule, FALSE AS is_aggregated_region, 'Canonical department region.' AS notes),
  STRUCT('MOQUEGUA' AS source_region, 'MOQUEGUA' AS region_canonical, 'department' AS region_scope, 'identity' AS mapping_rule, FALSE AS is_aggregated_region, 'Canonical department region.' AS notes),
  STRUCT('PASCO' AS source_region, 'PASCO' AS region_canonical, 'department' AS region_scope, 'identity' AS mapping_rule, FALSE AS is_aggregated_region, 'Canonical department region.' AS notes),
  STRUCT('PIURA' AS source_region, 'PIURA' AS region_canonical, 'department' AS region_scope, 'identity' AS mapping_rule, FALSE AS is_aggregated_region, 'Canonical department region.' AS notes),
  STRUCT('PUNO' AS source_region, 'PUNO' AS region_canonical, 'department' AS region_scope, 'identity' AS mapping_rule, FALSE AS is_aggregated_region, 'Canonical department region.' AS notes),
  STRUCT('SAN MARTIN' AS source_region, 'SAN MARTIN' AS region_canonical, 'department' AS region_scope, 'identity' AS mapping_rule, FALSE AS is_aggregated_region, 'Canonical department region; accents are normalized away.' AS notes),
  STRUCT('TACNA' AS source_region, 'TACNA' AS region_canonical, 'department' AS region_scope, 'identity' AS mapping_rule, FALSE AS is_aggregated_region, 'Canonical department region.' AS notes),
  STRUCT('TUMBES' AS source_region, 'TUMBES' AS region_canonical, 'department' AS region_scope, 'identity' AS mapping_rule, FALSE AS is_aggregated_region, 'Canonical department region.' AS notes),
  STRUCT('UCAYALI' AS source_region, 'UCAYALI' AS region_canonical, 'department' AS region_scope, 'identity' AS mapping_rule, FALSE AS is_aggregated_region, 'Canonical department region.' AS notes),
  STRUCT('CALLAO' AS source_region, 'CALLAO' AS region_canonical, 'province' AS region_scope, 'manual_callao_aggregation' AS mapping_rule, TRUE AS is_aggregated_region, 'Canonical Callao bucket used for regional predictive foundation.' AS notes),
  STRUCT('PROV. CONST. DEL CALLAO' AS source_region, 'CALLAO' AS region_canonical, 'province' AS region_scope, 'manual_callao_aggregation' AS mapping_rule, TRUE AS is_aggregated_region, 'Mapped to Callao for regional predictive foundation; variants are not modeled separately in v1.' AS notes),
  STRUCT('PROVINCIA CONSTITUCIONAL DEL CALLAO' AS source_region, 'CALLAO' AS region_canonical, 'province' AS region_scope, 'manual_callao_aggregation' AS mapping_rule, TRUE AS is_aggregated_region, 'Mapped to Callao for regional predictive foundation; variants are not modeled separately in v1.' AS notes)
]);
