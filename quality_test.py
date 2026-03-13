from classes.Utils import Utils


sql = '''
WITH
  -- ====================================================================
  -- 0. CONTROLE DE EXECUÇÃO
  -- ====================================================================
 
  parametros_execucao AS (
    SELECT
     floor(CAST( {anomesdia} AS INTEGER) / 100) AS input_anomes
  ),

  -- ====================================================================
  -- 1. DEFINIÇÃO DE DATAS E PARTIÇÕES
  -- ====================================================================
 
  -- Descobre a maior data (anomesdia) disponível DENTRO do mês solicitado
  ref_param_calc AS (
    SELECT
      p.input_anomes,
      (
        SELECT MAX(CAST(REPLACE(CAST(anomesdia AS VARCHAR), '-', '') AS INTEGER))
        FROM "db_corp_geracaoegestaodeinsights_dadosempresas_spec_01"."tbl_bupj_de_cadastro_super_cadastro"
        WHERE CAST(substring(REPLACE(CAST(anomesdia AS VARCHAR), '-', ''), 1, 6) AS INTEGER) = p.input_anomes
      ) AS max_dia_do_mes,
      (
        SELECT MAX(CAST(REPLACE(CAST(anomesdia AS VARCHAR), '-', '') AS INTEGER))
        FROM "db_corp_geracaoegestaodeinsights_dadosempresas_spec_01"."tbl_bupj_de_cadastro_super_cadastro"
      ) AS max_dia_geral
    FROM parametros_execucao p
  ),

  ref_param AS (
    SELECT
      COALESCE(calc.max_dia_do_mes, calc.max_dia_geral) AS ref_anomesdia,
      COALESCE(calc.input_anomes, CAST(SUBSTRING(CAST(calc.max_dia_geral AS VARCHAR), 1, 6) AS INTEGER)) AS ref_anomes
    FROM ref_param_calc calc
  ),

  params_datas AS (
    SELECT
      -- SUPERCAD: anomesdia - Maior data encontrada para o mês solicitado
      ref.ref_anomesdia AS max_dt_supercad,
     
      -- OPERADORES: anomesdia - Alinhado com o Supercad
      ref.ref_anomesdia AS max_dt_oper,
      ref.ref_anomesdia AS max_dt_oper_legado,
      ref.ref_anomesdia AS max_dt_perf,
     
      -- EQ3: anomesdia - Maior data disponível na tabela (independente do mês)
      (SELECT MAX(ano_mes_dia) FROM "db_corp_dadoscadastrais_cadastrointernomodernizado_sor_01"."tbeq3_gest_docm_cada_pess") AS max_dt_eq3,
     
      -- REDE: anomes - Alinhado com mês de referência
      ref.ref_anomes AS max_dt_cad_rede,
     
      -- ENGAJAMENTO: anomes - Regra M-4 ou Max disponível
      CASE
        WHEN CAST(ref.ref_anomes AS VARCHAR) IN (SELECT DISTINCT dt_particao FROM database_db_compartilhado_consumer_dungeonsedados.sot_tbl_engajamento_cnpj)
        THEN CAST(ref.ref_anomes AS VARCHAR)
        ELSE (SELECT MAX(dt_particao) FROM database_db_compartilhado_consumer_dungeonsedados.sot_tbl_engajamento_cnpj)
      END AS max_dt_engaj,
     
      -- PF: anomes - Tenta usar o mês correto, se não existir (lag M-2), usa o Max
      COALESCE(
        (SELECT MAX(anomes) FROM "database_db_compartilhado_consumer_umsoitausinergiapfpj"."th6_um_so_itau_pf_pj" WHERE anomes = ref.ref_anomes),
        (SELECT MAX(anomes) FROM "database_db_compartilhado_consumer_umsoitausinergiapfpj"."th6_um_so_itau_pf_pj")
      ) AS max_dt_pf,
     
      -- IDL (HISTÓRICO CONSOLIDADO): anomes - Maior partição disponível
      (SELECT MAX(anomes) FROM "workspace_db"."tb_nav_idl_hist") AS max_dt_tb_nav_idl_hist,
     
      -- Outras Mensais
      ref.ref_anomes AS max_dt_class_acesso,
     
      ref.ref_anomes AS ref_anomes_nav
     
    FROM ref_param ref
  ),

  -- ====================================================================
  -- 2. SUPERCAD (BASE CENTRAL)
  -- Partição: anomesdia - maior data dentro do mês solicitado
  -- ====================================================================
  supercad AS (
    SELECT
      id_chave_cliente,
      COALESCE(cod_cpfcnpj14_str, lpad(cast(num_cpfcnpj14 AS varchar), 14, '0')) AS cnpj14,
      COALESCE(LPAD(SUBSTR(cod_cpfcnpj14_str, 1, 8), 8, '0'), LPAD(SUBSTR(CAST(num_cpfcnpj14 AS VARCHAR), 1, 8), 8, '0')) AS cnpj8,
      des_negocio,
      dat_conta_abertura_efetiva,
      des_conta_status,
      dat_cliente_fundacao,
      nom_persona_jogo,
      nom_persona_jogador,
      anomesdia AS anomesdia_data,
      num_agencia,
      num_conta,
      num_conta_dac,
      des_conta_tipo,
      CASE WHEN des_cpfcnpj_status = 'CNPJ9 INATIVO' THEN 'INATIVO' WHEN des_cpfcnpj_status = 'CNPJ9 ATIVO' THEN 'ATIVO' ELSE des_cpfcnpj_status END AS des_cpfcnpj_status,
      des_cliente_endereco_estado,
      CAST(SUBSTRING(REPLACE(CAST(anomesdia AS VARCHAR), '-', ''), 1, 6) AS INTEGER) AS anomes,
      CAST(CONCAT(SUBSTRING(REPLACE(CAST(anomesdia AS VARCHAR), '-', ''), 1, 6), '01') AS INTEGER) AS anomesdia,
      CASE
        WHEN lower(des_chave_segmento) LIKE '%varejo%' THEN 'VAREJO'
        WHEN lower(des_chave_segmento) LIKE '%atacado%' THEN 'ATACADO'
        ELSE NULL
      END AS grupo_segmento,
      COALESCE(NULLIF(des_segmentacao, ''), NULL) AS segmentacao_detalhada
    FROM (
      SELECT *,
        ROW_NUMBER() OVER (
          PARTITION BY num_cpfcnpj14
          ORDER BY
             CASE
                  WHEN des_segmentacao IS NOT NULL
                   AND des_conta_status <> 'CONTA ENCERRADA'
                  THEN 0
                  ELSE 1
                END ASC,
                TRY_CAST(cod_prioridade_gx6_cpfcnpj14 AS INTEGER) ASC
        ) AS rn
      FROM "db_corp_geracaoegestaodeinsights_dadosempresas_spec_01"."tbl_bupj_de_cadastro_super_cadastro"
      WHERE CAST(REPLACE(CAST(anomesdia AS VARCHAR), '-', '') AS INTEGER) = (SELECT max_dt_supercad FROM params_datas)
       AND "cod_tipo_pessoa" = 'J'
    )
    WHERE rn = 1
  ),

  -- ====================================================================
  -- 3. EQ3
  -- Partição: anomesdia - maior data geral da tabela (snapshot mais recente absoluto)
  -- ====================================================================
  tb_eq3 AS (
    SELECT
        id_eq3,
        cnpj14,
        cod_tipo,
        tipo_pessoa,
        anomes
    FROM (
        SELECT
            cod_idef_pess AS id_eq3,
            lpad(cast(num_docm_pess AS varchar), 14, '0') AS cnpj14,
            regexp_extract(cod_chav_patc, 'BR#([0-9]+)#', 1) AS cod_tipo,
            CASE WHEN regexp_extract(cod_chav_patc, 'BR#([0-9]+)#', 1) = '1' THEN 'PF' ELSE 'PJ' END AS tipo_pessoa,
            FLOOR(ano_mes_dia / 100) AS anomes,
            -- Garante 1:1 por CNPJ pegando o ID mais recente caso haja duplicidade
            ROW_NUMBER() OVER(PARTITION BY num_docm_pess ORDER BY ano_mes_dia DESC) as rn
        FROM "db_corp_dadoscadastrais_cadastrointernomodernizado_sor_01"."tbeq3_gest_docm_cada_pess"
        WHERE ano_mes_dia = (SELECT max_dt_eq3 FROM params_datas)
    )
    WHERE tipo_pessoa = 'PJ' AND rn = 1
  ),

  -- ====================================================================
  -- 4. OPERADORES (UD)
  -- Partição: anomesdia - segue a data do Supercad para alinhamento
  -- ====================================================================
  tb_ud_unificada AS (
    SELECT
      cnpj14,
      cod_oper,
      cod_tip_oper,
      desc_tip_oper,
      desc_sit_oper,
      dat_cria_usua_plus,
      ROW_NUMBER() OVER (
        PARTITION BY cnpj14
        ORDER BY
         CASE WHEN cod_tip_oper IS NOT NULL AND desc_sit_oper = 'ATIVO' THEN 0 ELSE 1 END ASC,
         cod_oper DESC
      ) AS rank_interno_operador
    FROM (
      SELECT
        lpad(cast(num_cnpj AS varchar), 14, '0') AS cnpj14,
        cod_oper,
        cod_tip_oper,
        desc_tip_oper,
        desc_sit_oper,
        dat_cria_usua_plus,
        ROW_NUMBER() OVER (
          PARTITION BY lpad(cast(num_cnpj AS varchar), 14, '0'), cod_oper
          ORDER BY
            CASE WHEN cod_tip_oper IS NOT NULL OR cod_tip_oper <> '' THEN 0 ELSE 1 END ASC,
            cod_oper DESC
        ) AS rn_dedup_operador
      FROM (
        SELECT DISTINCT
          num_cnpj_clie AS num_cnpj,
          cod_usua_ctrt_plus AS cod_oper,
          cod_cate_perf_usua AS cod_tip_oper,
          CASE
            WHEN cod_cate_perf_usua = 'P3' THEN 'OPERADOR'
            WHEN cod_cate_perf_usua = 'P0' THEN 'REPRESENTANTE LEGAL'
            WHEN cod_cate_perf_usua = 'P1' THEN 'REPRESENTANTE DELEGADO'
            ELSE NULL
          END AS desc_tip_oper,
          CASE
            WHEN COD_SITU_CTRT = 0 THEN 'ATIVO'
            WHEN COD_SITU_CTRT = 1 THEN 'BLOQUEADO'
            WHEN COD_SITU_CTRT = 9 THEN 'EXCLUIDO'
            ELSE NULL
          END AS desc_sit_oper,
          CAST(SUBSTRING(dat_cria_usua_plus,1,10) AS DATE) AS dat_cria_usua_plus
        FROM "database_db_compartilhado_consumer_iamsepj"."sot_ud_operadores"
        WHERE CAST(REPLACE(CAST(anomesdia AS VARCHAR), '-', '') AS INTEGER) = (SELECT max_dt_oper FROM params_datas)
        UNION ALL
        SELECT DISTINCT
          num_cnpj,
          cod_oper,
          cod_tip_oper,
          COALESCE(desc_tip_oper, NULL) AS desc_tip_oper,
          desc_sit_oper,
          CAST(SUBSTRING(dat_cad_oper,1,10) AS DATE) AS dat_cria_usua_plus
        FROM "database_db_compartilhado_consumer_iamsepj"."sot_ud_operadores_legado"
        WHERE CAST(REPLACE(CAST(anomesdia AS VARCHAR), '-', '') AS INTEGER) = (SELECT max_dt_oper_legado FROM params_datas)
      ) AS raw_union
    ) AS dedup_step
    WHERE rn_dedup_operador = 1
  ),

  -- ====================================================================
  -- 5. BASE CONSOLIDADA
  -- Partição: Full Join mantendo dados de ambas as pontas
  -- ====================================================================
  base_consolidada AS (
    SELECT
      CASE
        WHEN sc.cnpj14 IS NOT NULL AND ud.cnpj14 IS NOT NULL THEN 'SUPERCAD_MATCH'
        WHEN sc.cnpj14 IS NOT NULL THEN 'SUPERCAD_ONLY'
        ELSE 'UD_ONLY'
      END AS origem_dado,
      COALESCE(sc.id_chave_cliente, CAST(NULL AS VARCHAR)) AS id_chave_cliente,
      COALESCE(sc.cnpj14, ud.cnpj14) AS cnpj14,
      COALESCE(sc.cnpj8, substr(ud.cnpj14, 1, 8)) AS cnpj8,
      sc.des_negocio,
      sc.dat_conta_abertura_efetiva,
      sc.des_conta_status,
      sc.dat_cliente_fundacao,
      sc.nom_persona_jogo,
      sc.nom_persona_jogador,
      sc.num_agencia,
      sc.num_conta,
      sc.num_conta_dac,
      sc.des_conta_tipo,
      sc.des_cpfcnpj_status,
      sc.des_cliente_endereco_estado,
      sc.grupo_segmento,
      sc.segmentacao_detalhada,
     
      -- Garante partição não nula
      COALESCE(sc.anomes, (SELECT ref_anomes FROM ref_param)) AS anomes,
      sc.anomesdia_data,
     
   
        COALESCE(
            sc.anomesdia,
            CAST(CONCAT(CAST((SELECT ref_anomes FROM ref_param) AS VARCHAR), '01') AS INT)
        ) AS anomesdia,

     
      ud.cod_oper,
      ud.cod_tip_oper,
      ud.desc_tip_oper,
      ud.desc_sit_oper,
      ud.dat_cria_usua_plus,
      COALESCE(ud.rank_interno_operador, 1) AS rank_interno_operador
    FROM supercad sc
    FULL OUTER JOIN tb_ud_unificada ud
      ON sc.cnpj14 = ud.cnpj14
  ),

  -- ====================================================================
  -- 6. ENGAJAMENTO
  -- Partição: anomes - lógica M-4 ou Max disponível
  -- ====================================================================
  tb_engajamento AS (
    SELECT
      lpad(substr(CAST(eng.num_cpf_cnpj AS VARCHAR), 1, 8), 8, '0') AS cnpj8,
      segmento AS segmento_engajamento,
      CASE
        WHEN eng.engajamento_novo IN ('1.desengajado', '2.engajado-') THEN 'Desengajado'
        WHEN eng.engajamento_novo = '3.engajado+' THEN 'Engajado'
        WHEN eng.engajamento_novo = '4.fidelizado' THEN 'Fidelizado'
        ELSE NULL
      END AS faixa_engajamento
    FROM database_db_compartilhado_consumer_dungeonsedados.sot_tbl_engajamento_cnpj eng
    WHERE eng.dt_particao = (SELECT max_dt_engaj FROM params_datas)
  ),

  -- ====================================================================
  -- 7. PERSONAS
  -- Partição: anomes - Max disponível
  -- ====================================================================
  tb_personas AS (
    SELECT
      lpad(cast(num_cnpj_empr as Varchar), 8, '0') AS cnpj8,
      nom_jogo AS nome_jogo_ws4,
      nom_clsr AS nome_jogador_ws4,
      nom_cate_tipo_clie,
      nom_cate_idad_empr,
      nom_grup_digl,
      cod_perf_risc_cred,
      ano_mes_patc
    FROM ws4.tbws4009_clsr_jogo_pjur
    WHERE ano_mes_patc = (SELECT MAX(ano_mes_patc) FROM ws4.tbws4009_clsr_jogo_pjur)
  ),

  -- ====================================================================
  -- 8. STATUS ATIVO
  -- Partição: anomesdia - segue data do Supercad
  -- ====================================================================
  cli_ativo_dedup AS (
    SELECT
      lpad(cast(eq3.num_docm_pess AS varchar), 14, '0') AS cnpj14,
      COALESCE(MAX(per_cli.ind_clie_ativ), 0) AS ind_clie_ativ
    FROM db_corp_dadoscadastrais_cadastrointernomodernizado_sor_01.tbeq3_gest_docm_cada_pess eq3
    LEFT JOIN db_corp_modelagem_visaoclientecrm_sot_01.tbjq8_vsao_perf_clie per_cli
      ON eq3.cod_idef_pess = per_cli.cod_chav_coro_clie
      AND per_cli.anomesdia = (SELECT max_dt_perf FROM params_datas)
    WHERE eq3.ano_mes_dia = (SELECT max_dt_eq3 FROM params_datas)
    GROUP BY 1
  ),

  -- ====================================================================
  -- 9. VÍNCULO PF
  -- Partição: anomes - Regra M-2 (tenta mês corrente, fallback para Max)
  -- ====================================================================
  pf_base_raw AS (
    SELECT
        num_cnpj14,
        tipo_sociedade_pf,
        qtd_socios,
        segmento_cliente_scio,
        porte_emp,
        rating_credito_emp,
        porte_scio,
        flag_vinc_rede_ativ_emp,
        flag_correntista_emp,
        flag_correntista_ativo_emp,
        flag_correntista_scio,
        flag_correntista_ativo_scio
    FROM database_db_compartilhado_consumer_umsoitausinergiapfpj.th6_um_so_itau_pf_pj
    WHERE anomes = (SELECT max_dt_pf FROM params_datas)
  ),
  vinculo_pf_agg AS (
    SELECT
        num_cnpj14 AS cnpj14,
        MAX(tipo_sociedade_pf)                        AS tipo_sociedade_pf,
        MAX(qtd_socios)                               AS qtd_socios,
        MAX(segmento_cliente_scio)                    AS segmento_cliente_scio,
        MAX(porte_emp)                                AS porte_emp,
        MAX(rating_credito_emp)                       AS rating_credito_emp,
        MAX(porte_scio)                               AS porte_scio,
        1                                             AS flag_vinculo_pf,
        MAX(COALESCE(flag_vinc_rede_ativ_emp, 0))     AS flag_vinc_rede_ativ_emp,
        MAX(COALESCE(flag_correntista_emp, 0))        AS flag_correntista_emp,
        MAX(COALESCE(flag_correntista_ativo_emp, 0))  AS flag_correntista_ativo_emp,
        MAX(COALESCE(flag_correntista_scio, 0))       AS flag_correntista_scio,
        MAX(COALESCE(flag_correntista_ativo_scio, 0)) AS flag_correntista_ativo_scio
    FROM pf_base_raw
    GROUP BY num_cnpj14
  ),

  -- ====================================================================
  -- 10. IDL NAVEGAÇÃO
  -- ====================================================================
 
  -- 10.1 BASE MENSAL e HISTÓRICA CONSOLIDADA (Consumindo tabela processada)
  -- Partição: anomes - Maior disponível na tabela de origem
  base_idl_raw AS (
    SELECT DISTINCT
        num_docm_pess as cnpj14,
        canal,
        anomes_primeiro_acesso
    FROM "workspace_db"."tb_nav_idl_hist"
    WHERE anomes = (SELECT max_dt_tb_nav_idl_hist FROM params_datas)
  ),
 
  -- 10.3 FLAGS HISTÓRICAS (Considera toda a base lida da maior partição)
  idl_stats_hist AS (
    SELECT
        cnpj14,
        MIN(anomes_primeiro_acesso) as primeiro_acesso_anomes_geral,
        MAX(1) AS flag_hist_geral,
        MAX(CASE WHEN canal = 'web' THEN 1 ELSE 0 END) as flag_idl_web
    FROM base_idl_raw
    group by 1
  ),

  -- ====================================================================
  -- 11. REDE
  -- Partição: anomes - alinhado com mês de referência
  -- ====================================================================

  flag_rede_mensal AS (
   SELECT DISTINCT
      cnpj14,
      anomes,
      1 AS flag_cliente_rede
    FROM (
      SELECT DISTINCT
        lpad(cast(num_cpfcnpj AS varchar), 14, '0') AS cnpj14,
        partition_column,
        CAST(SUBSTRING(REPLACE(CAST(partition_column AS VARCHAR), '-', ''), 1, 6) AS INTEGER) AS anomes
      FROM "database_db_compartilhado_consumer_analyticsrede"."tbl_redebupj_de_general_dataset_cadastro_segmentacao_itau"
      WHERE CAST(SUBSTRING(REPLACE(CAST(partition_column AS VARCHAR), '-', ''), 1, 6) AS INTEGER) = (SELECT max_dt_cad_rede FROM params_datas)
      )
    ),
 
  -- ====================================================================
  -- 12. CLASSIFICAÇÃO ACESSOS (GA4)
  -- Partição: anomes - alinhado com mês de referência
  -- ====================================================================
  clss_acessos_agg AS (
    SELECT DISTINCT
      floor(acess.anomesdia / 100) AS anomes,
      eq3.cnpj14,
      CASE
        WHEN (MAX(flag_acesso_app) = 1 AND MAX(flag_acesso_web) = 0) THEN 'Apenas Mobile'
        WHEN (MAX(flag_acesso_app) = 0 AND MAX(flag_acesso_web) = 1) THEN 'Apenas Bankline (App + Web)'
        WHEN (MAX(flag_acesso_hibrido_app_web) = 1 ) THEN 'Mobile E Bankline'
        WHEN (MAX(flag_acesso_ibba) = 1 ) THEN 'IBBA'
        WHEN (MAX(flag_acesso_site_nl) = 1 ) THEN 'Site NL'
        ELSE NULL
      END AS canal
    FROM "workspace_db"."tbl_classificacao_acesso" acess
    LEFT JOIN tb_eq3 eq3
      ON acess.id_eq3 = eq3.id_eq3
    WHERE floor(acess.anomesdia / 100) = (SELECT max_dt_class_acesso FROM params_datas)
    GROUP BY 1, 2
  )

-- ====================================================================
-- SELECT FINAL
-- ====================================================================
SELECT
  -- Chaves
  COALESCE(sc.rank_interno_operador, 1) AS rank_filtro_unico_cnpj,
  sc.origem_dado,
  COALESCE(sc.id_chave_cliente, eq3.id_eq3) AS id_chave_cliente,
  eq3.id_eq3,
  sc.cnpj14,
  sc.cnpj8,
 
  -- Operador
  sc.cod_oper AS cod_operador,
  sc.cod_tip_oper AS cod_tipo_operador,
  sc.desc_tip_oper AS tipo_operador,
  sc.desc_sit_oper AS situacao_oper,
  sc.dat_cria_usua_plus AS dat_cria_usua,
 
  -- Empresa
  sc.des_negocio,
  sc.grupo_segmento,
  sc.segmentacao_detalhada,
  sc.dat_conta_abertura_efetiva,
  sc.dat_cliente_fundacao,
  sc.des_conta_status,
  sc.num_agencia,
  sc.num_conta,
  sc.num_conta_dac,
  sc.des_conta_tipo,
  sc.des_cpfcnpj_status,
  sc.des_cliente_endereco_estado,
  sc.nom_persona_jogo AS persona_jogo_supercad,
  sc.nom_persona_jogador AS persona_jogador_supercad,
 
  -- Personas & Engajamento
  pers.nome_jogo_ws4,
  pers.nome_jogador_ws4,
  pers.nom_cate_tipo_clie,
  pers.nom_grup_digl AS perfil_digital,
  pers.cod_perf_risc_cred AS risco_credito,
  eng.faixa_engajamento AS faixa_engajamento,
  eng.segmento_engajamento AS segmento_engajamento,
 
  -- Status & Vínculos (PF)
  COALESCE(cli.ind_clie_ativ, 0) AS flag_cliente_ativo_tbjq8,
  COALESCE(vinc_pf.flag_vinculo_pf, 0) AS flag_tem_vinculo_pf,
  vinc_pf.tipo_sociedade_pf,
  vinc_pf.qtd_socios,
  vinc_pf.segmento_cliente_scio,
  vinc_pf.porte_emp,
  vinc_pf.rating_credito_emp,
  vinc_pf.porte_scio,

    CAST(vinc_pf.flag_vinc_rede_ativ_emp AS VARCHAR) AS flag_vinc_rede_ativ_emp,
    CAST(vinc_pf.flag_correntista_emp AS VARCHAR) AS flag_correntista_emp,
    CAST(vinc_pf.flag_correntista_ativo_emp AS VARCHAR) AS flag_correntista_ativo_emp,
    CAST(vinc_pf.flag_correntista_scio AS VARCHAR) AS flag_correntista_scio,
    CAST(vinc_pf.flag_correntista_ativo_scio AS VARCHAR) AS flag_correntista_ativo_scio,

  -- Canal Predominante
  acess.canal AS canal_acessos,
 
 
--   -- IDL: Flags Históricas

  CAST(COALESCE(idl_hist_cnpj.flag_idl_web, 0) AS VARCHAR) AS flag_idl_cnpj_web_hist,
  idl_hist_cnpj.primeiro_acesso_anomes_geral AS anomes_primeiro_acesso_idl,
 
  -- Rede
  CAST(COALESCE(rede.flag_cliente_rede, 0) AS VARCHAR) AS flag_cliente_rede,

  -- Clusters
  CASE
    WHEN sc.dat_cliente_fundacao IS NULL THEN 'Data Nao Disponivel'
    WHEN date_diff('year', CAST(sc.dat_cliente_fundacao AS DATE), sc.anomesdia_data) < 2 THEN 'Menos de 2 anos'
    WHEN date_diff('year', CAST(sc.dat_cliente_fundacao AS DATE), sc.anomesdia_data) BETWEEN 2 AND 5 THEN 'De 2 a 5 anos'
    WHEN date_diff('year', CAST(sc.dat_cliente_fundacao AS DATE), sc.anomesdia_data) BETWEEN 6 AND 10 THEN 'De 6 a 10 anos'
    WHEN date_diff('year', CAST(sc.dat_cliente_fundacao AS DATE), sc.anomesdia_data) BETWEEN 11 AND 20 THEN 'De 11 a 20 anos'
    WHEN date_diff('year', CAST(sc.dat_cliente_fundacao AS DATE), sc.anomesdia_data) BETWEEN 21 AND 50 THEN 'De 21 a 50 anos'
    WHEN date_diff('year', CAST(sc.dat_cliente_fundacao AS DATE), sc.anomesdia_data) BETWEEN 51 AND 100 THEN 'De 51 a 100 anos'
    ELSE 'Mais de 100 anos'
  END AS cluster_tempo_vida_empresa,
 
  -- Relacionamentos
  CASE
    WHEN MAX(COALESCE(sc.rank_interno_operador, 1)) OVER (PARTITION BY sc.cnpj14) = 1 THEN '1:1' ELSE '1:N'
  END AS relacionamento_cnpj_operadores,
  MAX(COALESCE(sc.rank_interno_operador, 1)) OVER (PARTITION BY sc.cnpj14) AS qtd_operadores_cnpj,
 
  CASE
    WHEN sc.cod_oper IS NULL THEN NULL
   

    WHEN COUNT(sc.cnpj14) OVER (PARTITION BY sc.cod_oper) = 1 THEN '1:1'
    ELSE '1:N'
  END AS relacionamento_operador_cnpjs,
  CASE
    WHEN sc.cod_oper IS NULL THEN NULL
    ELSE COUNT(sc.cnpj14) OVER (PARTITION BY sc.cod_oper)
  END AS qtd_cnpjs_operador,

  sc.anomesdia_data,
  sc.anomes,
  sc.anomesdia

FROM base_consolidada sc
LEFT JOIN tb_engajamento eng ON sc.cnpj8 = eng.cnpj8
LEFT JOIN tb_personas pers ON sc.cnpj8 = pers.cnpj8
LEFT JOIN cli_ativo_dedup cli ON sc.cnpj14 = cli.cnpj14
LEFT JOIN vinculo_pf_agg vinc_pf ON sc.cnpj14 = vinc_pf.cnpj14
LEFT JOIN tb_eq3 eq3 ON sc.cnpj14 = eq3.cnpj14
LEFT JOIN clss_acessos_agg acess ON sc.cnpj14 = acess.cnpj14
LEFT JOIN flag_rede_mensal AS rede ON sc.cnpj14 = rede.cnpj14 AND sc.anomes = rede.anomes

-- -- Join IDL (Por CNPJ)
LEFT JOIN idl_stats_hist idl_hist_cnpj ON sc.cnpj14 = idl_hist_cnpj.cnpj14
'''


origens = Utils.get_origens_sql(sql = sql)


# add_source(self, db: str, table: str, partition_type: str, partition_name: str, partition_value: Any)


for origem in origens:
    db = origem.get('db')
    table = origem.get('table')
    conplete_path = origem.get('path')

    
    



#print(origens)

from classes.GlueManager import GlueManager 

glue = GlueManager()

glue.get_description_table(db ='workspace_db'.table_name = 'tbl_classificacao_acesso')