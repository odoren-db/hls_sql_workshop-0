-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###beneficiary_summary

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_beneficiary_summary
AS
SELECT
     md5(bs.DESYNPUF_ID||current_timestamp||bs._metadata.file_name) as beneficiary_key
    ,bs.DESYNPUF_ID as beneficiary_code
    ,cast(substring(bs._metadata.file_name,7,4) as int) as year
    ,to_date(bs.BENE_BIRTH_DT,'yyyyMMdd') as date_of_birth
    ,to_date(bs.BENE_DEATH_DT,'yyyyMMdd') as date_of_death
    ,cast(l_BENE_SEX_IDENT_CD.label as string) as gender
    ,cast(l_BENE_RACE_CD.label as string) as race
    ,CASE WHEN BENE_ESRD_IND = 'Y' THEN 'Yes' ELSE 'No' END as esrd_flag
    ,cast(l_SP_STATE_CODE.label as string) as state
    ,bs.BENE_COUNTY_CD as county_code
    ,cast(bs.BENE_HI_CVRAGE_TOT_MONS as int) as part_a_coverage_months
    ,cast(bs.BENE_SMI_CVRAGE_TOT_MONS as int) as part_b_coverage_months
    ,cast(bs.BENE_HMO_CVRAGE_TOT_MONS as int) as hmo_coverage_months
    ,cast(bs.PLAN_CVRG_MOS_NUM as int) as part_d_coverage_months
    ,cast(l_SP_ALZHDMTA.label as string) as alzheimers_or_related_flag
    ,cast(l_SP_CHF.label as string) as heart_failure_flag
    ,cast(l_SP_CHRNKIDN.label as string) as cronic_kidney_disease_flag
    ,cast(l_SP_CNCR.label as string) as cancer_flag
    ,cast(l_SP_COPD.label as string) as copd_flag
    ,cast(l_SP_DEPRESSN.label as string) as depression_flag
    ,cast(l_SP_DIABETES.label as string) as diabetes_flag
    ,cast(l_SP_ISCHMCHT.label as string) as ischemic_heart_disease_flag
    ,cast(l_SP_OSTEOPRS.label as string) as osteoporosis_flag
    ,cast(l_SP_RA_OA.label as string) asrheumatoid_arthritis_flag
    ,cast(l_SP_STRKETIA.label as string) as stroke_transient_ischemic_attack_flag
    ,cast(MEDREIMB_IP as double) as inpatient_annual_medicare_reimbursement_amount
    ,cast(BENRES_IP as double) as inpatient_annual_beneficiary_responsibility_amount
    ,cast(PPPYMT_IP as double) as inpatient_annual_payer_reimbursement_amount
    ,cast(MEDREIMB_OP as double) as outpatient_institutional_annual_medicare_reimbursement_amount
    ,cast(BENRES_OP as double) as outpatient_institutional_annual_beneficiary_responsibiliy_amount
    ,cast(PPPYMT_OP as double) as outpatient_institutional_annual_primary_payer_reimbursement_amount
    ,cast(MEDREIMB_CAR as double) as carrier_annual_medicare_reimbursement_amount
    ,cast(BENRES_CAR as double) as carrier_annual_beneficiary_responsiblity_amount
    ,cast(PPPYMT_CAR as double) as carrier_annual_primary_payer_reimbursement_amount
    ,bs.load_timestamp as bronze_load_timestamp
    ,current_timestamp as load_timestamp 
FROM 
stream(live.bronze_beneficiary_summary) bs
left join live.bronze_lookup l_BENE_SEX_IDENT_CD on bs.BENE_SEX_IDENT_CD = l_BENE_SEX_IDENT_CD.code and l_BENE_SEX_IDENT_CD.variable = 'BENE_SEX_IDENT_CD'
left join live.bronze_lookup l_BENE_RACE_CD on bs.BENE_RACE_CD = l_BENE_RACE_CD.code and l_BENE_RACE_CD.variable = 'BENE_RACE_CD'
left join live.bronze_lookup l_SP_STATE_CODE on bs.SP_STATE_CODE = l_SP_STATE_CODE.code and l_SP_STATE_CODE.variable = 'SP_STATE_CODE'
left join live.bronze_lookup l_SP_ALZHDMTA on bs.SP_ALZHDMTA = l_SP_ALZHDMTA.code and l_SP_ALZHDMTA.variable = 'SP_ALZHDMTA'
left join live.bronze_lookup l_SP_CHF on bs.SP_CHF = l_SP_CHF.code and l_SP_CHF.variable = 'SP_CHF'
left join live.bronze_lookup l_SP_CHRNKIDN on bs.SP_CHRNKIDN = l_SP_CHRNKIDN.code and l_SP_CHRNKIDN.variable = 'SP_CHRNKIDN'
left join live.bronze_lookup l_SP_CNCR on bs.SP_CNCR = l_SP_CNCR.code and l_SP_CNCR.variable = 'SP_CNCR'
left join live.bronze_lookup l_SP_COPD on bs.SP_COPD = l_SP_COPD.code and l_SP_COPD.variable = 'SP_COPD'
left join live.bronze_lookup l_SP_DEPRESSN on bs.SP_DEPRESSN = l_SP_DEPRESSN.code and l_SP_DEPRESSN.variable = 'SP_DEPRESSN'
left join live.bronze_lookup l_SP_DIABETES on bs.SP_DIABETES = l_SP_DIABETES.code and l_SP_DIABETES.variable = 'SP_DIABETES'
left join live.bronze_lookup l_SP_ISCHMCHT on bs.SP_ISCHMCHT = l_SP_ISCHMCHT.code and l_SP_ISCHMCHT.variable = 'SP_ISCHMCHT'
left join live.bronze_lookup l_SP_OSTEOPRS on bs.SP_OSTEOPRS = l_SP_OSTEOPRS.code and l_SP_OSTEOPRS.variable = 'SP_OSTEOPRS'
left join live.bronze_lookup l_SP_RA_OA on bs.SP_RA_OA = l_SP_RA_OA.code and l_SP_RA_OA.variable = 'SP_RA_OA'
left join live.bronze_lookup l_SP_STRKETIA on bs.SP_STRKETIA = l_SP_STRKETIA.code and l_SP_STRKETIA.variable = 'SP_STRKETIA'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Carrier Claims

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_carrier_claims(
  CONSTRAINT `Beneficiary code is not null`    EXPECT (beneficiary_code is not null)
)
--PARTITIONED BY (file_name)
AS
SELECT
   cc.DESYNPUF_ID as beneficiary_code
  ,cc.CLM_ID as claim_id
  ,to_date(CLM_FROM_DT,'yyyyMMdd') as claim_start_date
  ,to_date(CLM_THRU_DT,'yyyyMMdd') as claim_end_date
  ,cc.ICD9_DGNS_CD_1 as claim_diagnosis_code_1
  ,cc.ICD9_DGNS_CD_2 as claim_diagnosis_code_2
  ,cc.ICD9_DGNS_CD_3 as claim_diagnosis_code_3
  ,cc.ICD9_DGNS_CD_4 as claim_diagnosis_code_4
  ,cc.ICD9_DGNS_CD_5 as claim_diagnosis_code_5
  ,cc.ICD9_DGNS_CD_6 as claim_diagnosis_code_6
  ,cc.ICD9_DGNS_CD_7 as claim_diagnosis_code_7
  ,cc.ICD9_DGNS_CD_8 as claim_diagnosis_code_8
  ,cc.PRF_PHYSN_NPI_1 as provider_physician_npi_1
  ,cc.PRF_PHYSN_NPI_2 as provider_physician_npi_2
  ,cc.PRF_PHYSN_NPI_3 as provider_physician_npi_3
  ,cc.PRF_PHYSN_NPI_4 as provider_physician_npi_4
  ,cc.PRF_PHYSN_NPI_5 as provider_physician_npi_5
  ,cc.PRF_PHYSN_NPI_6 as provider_physician_npi_6
  ,cc.PRF_PHYSN_NPI_7 as provider_physician_npi_7
  ,cc.PRF_PHYSN_NPI_8 as provider_physician_npi_8
  ,cc.PRF_PHYSN_NPI_9 as provider_physician_npi_9
  ,cc.PRF_PHYSN_NPI_10 as provider_physician_npi_10
  ,cc.PRF_PHYSN_NPI_11 as provider_physician_npi_11
  ,cc.PRF_PHYSN_NPI_12 as provider_physician_npi_12
  ,cc.PRF_PHYSN_NPI_13 as provider_physician_npi_13
  ,cc.TAX_NUM_1 as provider_insitution_tax_number_1
  ,cc.TAX_NUM_2 as provider_insitution_tax_number_2
  ,cc.TAX_NUM_3 as provider_insitution_tax_number_3
  ,cc.TAX_NUM_4 as provider_insitution_tax_number_4
  ,cc.TAX_NUM_5 as provider_insitution_tax_number_5
  ,cc.TAX_NUM_6 as provider_insitution_tax_number_6
  ,cc.TAX_NUM_7 as provider_insitution_tax_number_7
  ,cc.TAX_NUM_8 as provider_insitution_tax_number_8
  ,cc.TAX_NUM_9 as provider_insitution_tax_number_9
  ,cc.TAX_NUM_10 as provider_insitution_tax_number_10
  ,cc.TAX_NUM_11 as provider_insitution_tax_number_11
  ,cc.TAX_NUM_12 as provider_insitution_tax_number_12
  ,cc.TAX_NUM_13 as provider_insitution_tax_number_13
  ,cc.HCPCS_CD_1 as hcpcs_code_1
  ,cc.HCPCS_CD_2 as hcpcs_code_2
  ,cc.HCPCS_CD_3 as hcpcs_code_3
  ,cc.HCPCS_CD_4 as hcpcs_code_4
  ,cc.HCPCS_CD_5 as hcpcs_code_5
  ,cc.HCPCS_CD_6 as hcpcs_code_6
  ,cc.HCPCS_CD_7 as hcpcs_code_7
  ,cc.HCPCS_CD_8 as hcpcs_code_8
  ,cc.HCPCS_CD_9 as hcpcs_code_9
  ,cc.HCPCS_CD_10 as hcpcs_code_10
  ,cc.HCPCS_CD_11 as hcpcs_code_11
  ,cc.HCPCS_CD_12 as hcpcs_code_12
  ,cc.HCPCS_CD_13 as hcpcs_code_13
  ,cast(cc.LINE_NCH_PMT_AMT_1 as double) as nch_payment_amount_1
  ,cast(cc.LINE_NCH_PMT_AMT_2 as double) as nch_payment_amount_2
  ,cast(cc.LINE_NCH_PMT_AMT_3 as double) as nch_payment_amount_3
  ,cast(cc.LINE_NCH_PMT_AMT_4 as double) as nch_payment_amount_4
  ,cast(cc.LINE_NCH_PMT_AMT_5 as double) as nch_payment_amount_5
  ,cast(cc.LINE_NCH_PMT_AMT_6 as double) as nch_payment_amount_6
  ,cast(cc.LINE_NCH_PMT_AMT_7 as double) as nch_payment_amount_7
  ,cast(cc.LINE_NCH_PMT_AMT_8 as double) as nch_payment_amount_8
  ,cast(cc.LINE_NCH_PMT_AMT_9 as double) as nch_payment_amount_9
  ,cast(cc.LINE_NCH_PMT_AMT_10 as double) as nch_payment_amount_10
  ,cast(cc.LINE_NCH_PMT_AMT_11 as double) as nch_payment_amount_11
  ,cast(cc.LINE_NCH_PMT_AMT_12 as double) as nch_payment_amount_12
  ,cast(cc.LINE_NCH_PMT_AMT_13 as double) as nch_payment_amount_13
  ,cast(cc.LINE_BENE_PTB_DDCTBL_AMT_1 as double) as line_beneficiary_part_b_deductable_amount_1
  ,cast(cc.LINE_BENE_PTB_DDCTBL_AMT_2 as double) as line_beneficiary_part_b_deductable_amount_2
  ,cast(cc.LINE_BENE_PTB_DDCTBL_AMT_3 as double) as line_beneficiary_part_b_deductable_amount_3
  ,cast(cc.LINE_BENE_PTB_DDCTBL_AMT_4 as double) as line_beneficiary_part_b_deductable_amount_4
  ,cast(cc.LINE_BENE_PTB_DDCTBL_AMT_5 as double) as line_beneficiary_part_b_deductable_amount_5
  ,cast(cc.LINE_BENE_PTB_DDCTBL_AMT_6 as double) as line_beneficiary_part_b_deductable_amount_6
  ,cast(cc.LINE_BENE_PTB_DDCTBL_AMT_7 as double) as line_beneficiary_part_b_deductable_amount_7
  ,cast(cc.LINE_BENE_PTB_DDCTBL_AMT_8 as double) as line_beneficiary_part_b_deductable_amount_8
  ,cast(cc.LINE_BENE_PTB_DDCTBL_AMT_9 as double) as line_beneficiary_part_b_deductable_amount_9
  ,cast(cc.LINE_BENE_PTB_DDCTBL_AMT_10 as double) as line_beneficiary_part_b_deductable_amount_10
  ,cast(cc.LINE_BENE_PTB_DDCTBL_AMT_11 as double) as line_beneficiary_part_b_deductable_amount_11
  ,cast(cc.LINE_BENE_PTB_DDCTBL_AMT_12 as double) as line_beneficiary_part_b_deductable_amount_12
  ,cast(cc.LINE_BENE_PTB_DDCTBL_AMT_13 as double) as line_beneficiary_part_b_deductable_amount_13
  ,cast(cc.LINE_BENE_PRMRY_PYR_PD_AMT_1 as double) as line_beneficiary_primary_payer_paid_amount_1
  ,cast(cc.LINE_BENE_PRMRY_PYR_PD_AMT_2 as double) as line_beneficiary_primary_payer_paid_amount_2
  ,cast(cc.LINE_BENE_PRMRY_PYR_PD_AMT_3 as double) as line_beneficiary_primary_payer_paid_amount_3
  ,cast(cc.LINE_BENE_PRMRY_PYR_PD_AMT_4 as double) as line_beneficiary_primary_payer_paid_amount_4
  ,cast(cc.LINE_BENE_PRMRY_PYR_PD_AMT_5 as double) as line_beneficiary_primary_payer_paid_amount_5
  ,cast(cc.LINE_BENE_PRMRY_PYR_PD_AMT_6 as double) as line_beneficiary_primary_payer_paid_amount_6
  ,cast(cc.LINE_BENE_PRMRY_PYR_PD_AMT_7 as double) as line_beneficiary_primary_payer_paid_amount_7
  ,cast(cc.LINE_BENE_PRMRY_PYR_PD_AMT_8 as double) as line_beneficiary_primary_payer_paid_amount_8
  ,cast(cc.LINE_BENE_PRMRY_PYR_PD_AMT_9 as double) as line_beneficiary_primary_payer_paid_amount_9
  ,cast(cc.LINE_BENE_PRMRY_PYR_PD_AMT_10 as double) as line_beneficiary_primary_payer_paid_amount_10
  ,cast(cc.LINE_BENE_PRMRY_PYR_PD_AMT_11 as double) as line_beneficiary_primary_payer_paid_amount_11
  ,cast(cc.LINE_BENE_PRMRY_PYR_PD_AMT_12 as double) as line_beneficiary_primary_payer_paid_amount_12
  ,cast(cc.LINE_BENE_PRMRY_PYR_PD_AMT_13 as double) as line_beneficiary_primary_payer_paid_amount_13
  ,cast(cc.LINE_COINSRNC_AMT_1 as double) as line_coinsurance_amount_1
  ,cast(cc.LINE_COINSRNC_AMT_2 as double) as line_coinsurance_amount_2
  ,cast(cc.LINE_COINSRNC_AMT_3 as double) as line_coinsurance_amount_3
  ,cast(cc.LINE_COINSRNC_AMT_4 as double) as line_coinsurance_amount_4
  ,cast(cc.LINE_COINSRNC_AMT_5 as double) as line_coinsurance_amount_5
  ,cast(cc.LINE_COINSRNC_AMT_6 as double) as line_coinsurance_amount_6
  ,cast(cc.LINE_COINSRNC_AMT_7 as double) as line_coinsurance_amount_7
  ,cast(cc.LINE_COINSRNC_AMT_8 as double) as line_coinsurance_amount_8
  ,cast(cc.LINE_COINSRNC_AMT_9 as double) as line_coinsurance_amount_9
  ,cast(cc.LINE_COINSRNC_AMT_10 as double) as line_coinsurance_amount_10
  ,cast(cc.LINE_COINSRNC_AMT_11 as double) as line_coinsurance_amount_11
  ,cast(cc.LINE_COINSRNC_AMT_12 as double) as line_coinsurance_amount_12
  ,cast(cc.LINE_COINSRNC_AMT_13 as double) as line_coinsurance_amount_13
  ,cast(cc.LINE_ALOWD_CHRG_AMT_1 as double) as line_allowed_charge_amount_1
  ,cast(cc.LINE_ALOWD_CHRG_AMT_2 as double) as line_allowed_charge_amount_2
  ,cast(cc.LINE_ALOWD_CHRG_AMT_3 as double) as line_allowed_charge_amount_3
  ,cast(cc.LINE_ALOWD_CHRG_AMT_4 as double) as line_allowed_charge_amount_4
  ,cast(cc.LINE_ALOWD_CHRG_AMT_5 as double) as line_allowed_charge_amount_5
  ,cast(cc.LINE_ALOWD_CHRG_AMT_6 as double) as line_allowed_charge_amount_6
  ,cast(cc.LINE_ALOWD_CHRG_AMT_7 as double) as line_allowed_charge_amount_7
  ,cast(cc.LINE_ALOWD_CHRG_AMT_8 as double) as line_allowed_charge_amount_8
  ,cast(cc.LINE_ALOWD_CHRG_AMT_9 as double) as line_allowed_charge_amount_9
  ,cast(cc.LINE_ALOWD_CHRG_AMT_10 as double) as line_allowed_charge_amount_10
  ,cast(cc.LINE_ALOWD_CHRG_AMT_11 as double) as line_allowed_charge_amount_11
  ,cast(cc.LINE_ALOWD_CHRG_AMT_12 as double) as line_allowed_charge_amount_12
  ,cast(cc.LINE_ALOWD_CHRG_AMT_13 as double) as line_allowed_charge_amount_13
  ,cast(cc.LINE_PRCSG_IND_CD_1 as string) as line_processing_indicator_code_1
  ,cast(cc.LINE_PRCSG_IND_CD_2 as string) as line_processing_indicator_code_2
  ,cast(cc.LINE_PRCSG_IND_CD_3 as string) as line_processing_indicator_code_3
  ,cast(cc.LINE_PRCSG_IND_CD_4 as string) as line_processing_indicator_code_4
  ,cast(cc.LINE_PRCSG_IND_CD_5 as string) as line_processing_indicator_code_5
  ,cast(cc.LINE_PRCSG_IND_CD_6 as string) as line_processing_indicator_code_6
  ,cast(cc.LINE_PRCSG_IND_CD_7 as string) as line_processing_indicator_code_7
  ,cast(cc.LINE_PRCSG_IND_CD_8 as string) as line_processing_indicator_code_8
  ,cast(cc.LINE_PRCSG_IND_CD_9 as string) as line_processing_indicator_code_9
  ,cast(cc.LINE_PRCSG_IND_CD_10 as string) as line_processing_indicator_code_10
  ,cast(cc.LINE_PRCSG_IND_CD_11 as string) as line_processing_indicator_code_11
  ,cast(cc.LINE_PRCSG_IND_CD_12 as string) as line_processing_indicator_code_12
  ,cast(cc.LINE_PRCSG_IND_CD_13 as string) as line_processing_indicator_code_13
  ,cast(cc.LINE_ICD9_DGNS_CD_1 as string) as line_icd9_diagnosis_code_1
  ,cast(cc.LINE_ICD9_DGNS_CD_2 as string) as line_icd9_diagnosis_code_2
  ,cast(cc.LINE_ICD9_DGNS_CD_3 as string) as line_icd9_diagnosis_code_3
  ,cast(cc.LINE_ICD9_DGNS_CD_4 as string) as line_icd9_diagnosis_code_4
  ,cast(cc.LINE_ICD9_DGNS_CD_5 as string) as line_icd9_diagnosis_code_5
  ,cast(cc.LINE_ICD9_DGNS_CD_6 as string) as line_icd9_diagnosis_code_6
  ,cast(cc.LINE_ICD9_DGNS_CD_7 as string) as line_icd9_diagnosis_code_7
  ,cast(cc.LINE_ICD9_DGNS_CD_8 as string) as line_icd9_diagnosis_code_8
  ,cast(cc.LINE_ICD9_DGNS_CD_9 as string) as line_icd9_diagnosis_code_9
  ,cast(cc.LINE_ICD9_DGNS_CD_10 as string) as line_icd9_diagnosis_code_10
  ,cast(cc.LINE_ICD9_DGNS_CD_11 as string) as line_icd9_diagnosis_code_11
  ,cast(cc.LINE_ICD9_DGNS_CD_12 as string) as line_icd9_diagnosis_code_12
  ,cast(cc.LINE_ICD9_DGNS_CD_13 as string) as line_icd9_diagnosis_code_13
  ,cc.update_timestamp as bronze_update_timestamp
  ,current_timestamp as load_timestamp
  ,_metadata.file_name as file_name
FROM stream(live.bronze_carrier_claims) cc


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Inpatient Claims

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_inpatient_claims
AS
SELECT
   ic.DESYNPUF_ID as beneficiary_code
  ,ic.CLM_ID as claim_id
  ,cast(ic.SEGMENT as int) as claim_line_segment
  ,to_date(ic.CLM_FROM_DT,'yyyyMMdd') as claim_start_date
  ,to_date(ic.CLM_THRU_DT,'yyyyMMdd') as claim_end_date
  ,cast(ic.PRVDR_NUM as string) as provider_instituation_code
  ,cast(CLM_PMT_AMT as double) as claim_payment_amount
  ,cast(NCH_PRMRY_PYR_CLM_PD_AMT as double) as primary_payer_claim_paid_amount
  ,cast(AT_PHYSN_NPI as string) as attending_physician_npi
  ,cast(OP_PHYSN_NPI as string) as operating_physician_npi
  ,cast(OT_PHYSN_NPI as string) as other_physician_npi
  ,to_date(ic.CLM_ADMSN_DT,'yyyyMMdd') as inpatient_admission_date
  ,cast(ADMTNG_ICD9_DGNS_CD as string) as icd9_admitting_diagnosis_code
  ,cast(CLM_PASS_THRU_PER_DIEM_AMT as double) as claim_pass_through_per_diem_amount
  ,coalesce(cast(NCH_BENE_IP_DDCTBL_AMT as double),0.0) as beneficiary_inpatient_deductible_amount
  ,cast(ic.NCH_BENE_PTA_COINSRNC_LBLTY_AM as double) as nch_beneficiary_part_a_coinsurance_liability_limit
  ,cast(ic.NCH_BENE_BLOOD_DDCTBL_LBLTY_AM as double) as hch_beneficiary_blood_deductible_liabiliy_limit
  ,cast(ic.CLM_UTLZTN_DAY_CNT as int) as claim_utilization_day_count
  ,to_date(ic.NCH_BENE_DSCHRG_DT,'yyyyMMdd') as inpatient_discharged_date
  ,cast(ic.CLM_DRG_CD as string) as diagnosis_related_group_code
  ,cast(ic.ICD9_DGNS_CD_1 as string) as icd9_diagnosis_code_1
  ,cast(ic.ICD9_DGNS_CD_2 as string) as icd9_diagnosis_code_2
  ,cast(ic.ICD9_DGNS_CD_3 as string) as icd9_diagnosis_code_3
  ,cast(ic.ICD9_DGNS_CD_4 as string) as icd9_diagnosis_code_4
  ,cast(ic.ICD9_DGNS_CD_5 as string) as icd9_diagnosis_code_5
  ,cast(ic.ICD9_DGNS_CD_6 as string) as icd9_diagnosis_code_6
  ,cast(ic.ICD9_DGNS_CD_7 as string) as icd9_diagnosis_code_7
  ,cast(ic.ICD9_DGNS_CD_8 as string) as icd9_diagnosis_code_8
  ,cast(ic.ICD9_DGNS_CD_9 as string) as icd9_diagnosis_code_9
  ,cast(ic.ICD9_DGNS_CD_10 as string) as icd9_diagnosis_code_10
  ,cast(ic.ICD9_PRCDR_CD_1 as string) as icd9_procedure_code_1  
  ,cast(ic.ICD9_PRCDR_CD_2 as string) as icd9_procedure_code_2
  ,cast(ic.ICD9_PRCDR_CD_3 as string) as icd9_procedure_code_3
  ,cast(ic.ICD9_PRCDR_CD_4 as string) as icd9_procedure_code_4 
  ,cast(ic.ICD9_PRCDR_CD_5 as string) as icd9_procedure_code_5 
  ,cast(ic.ICD9_PRCDR_CD_6 as string) as icd9_procedure_code_6
  ,cast(ic.HCPCS_CD_1 as string) as hcfa_procedure_code_1
  ,cast(ic.HCPCS_CD_2 as string) as hcfa_procedure_code_2
  ,cast(ic.HCPCS_CD_3 as string) as hcfa_procedure_code_3
  ,cast(ic.HCPCS_CD_4 as string) as hcfa_procedure_code_4
  ,cast(ic.HCPCS_CD_5 as string) as hcfa_procedure_code_5
  ,cast(ic.HCPCS_CD_6 as string) as hcfa_procedure_code_6
  ,cast(ic.HCPCS_CD_7 as string) as hcfa_procedure_code_7
  ,cast(ic.HCPCS_CD_8 as string) as hcfa_procedure_code_8
  ,cast(ic.HCPCS_CD_9 as string) as hcfa_procedure_code_9
  ,cast(ic.HCPCS_CD_10 as string) as hcfa_procedure_code_10
  ,cast(ic.HCPCS_CD_11 as string) as hcfa_procedure_code_11
  ,cast(ic.HCPCS_CD_12 as string) as hcfa_procedure_code_12
  ,cast(ic.HCPCS_CD_13 as string) as hcfa_procedure_code_13
  ,cast(ic.HCPCS_CD_14 as string) as hcfa_procedure_code_14
  ,cast(ic.HCPCS_CD_15 as string) as hcfa_procedure_code_15
  ,cast(ic.HCPCS_CD_16 as string) as hcfa_procedure_code_16
  ,cast(ic.HCPCS_CD_17 as string) as hcfa_procedure_code_17
  ,cast(ic.HCPCS_CD_18 as string) as hcfa_procedure_code_18
  ,cast(ic.HCPCS_CD_19 as string) as hcfa_procedure_code_19
  ,cast(ic.HCPCS_CD_20 as string) as hcfa_procedure_code_20
  ,cast(ic.HCPCS_CD_21 as string) as hcfa_procedure_code_21
  ,cast(ic.HCPCS_CD_22 as string) as hcfa_procedure_code_22
  ,cast(ic.HCPCS_CD_23 as string) as hcfa_procedure_code_23
  ,cast(ic.HCPCS_CD_24 as string) as hcfa_procedure_code_24
  ,cast(ic.HCPCS_CD_25 as string) as hcfa_procedure_code_25
  ,cast(ic.HCPCS_CD_26 as string) as hcfa_procedure_code_26
  ,cast(ic.HCPCS_CD_27 as string) as hcfa_procedure_code_27
  ,cast(ic.HCPCS_CD_28 as string) as hcfa_procedure_code_28
  ,cast(ic.HCPCS_CD_29 as string) as hcfa_procedure_code_29
  ,cast(ic.HCPCS_CD_30 as string) as hcfa_procedure_code_30
  ,cast(ic.HCPCS_CD_31 as string) as hcfa_procedure_code_31
  ,cast(ic.HCPCS_CD_32 as string) as hcfa_procedure_code_32
  ,cast(ic.HCPCS_CD_33 as string) as hcfa_procedure_code_33
  ,cast(ic.HCPCS_CD_34 as string) as hcfa_procedure_code_34
  ,cast(ic.HCPCS_CD_35 as string) as hcfa_procedure_code_35
  ,cast(ic.HCPCS_CD_36 as string) as hcfa_procedure_code_36
  ,cast(ic.HCPCS_CD_37 as string) as hcfa_procedure_code_37
  ,cast(ic.HCPCS_CD_38 as string) as hcfa_procedure_code_38
  ,cast(ic.HCPCS_CD_39 as string) as hcfa_procedure_code_39
  ,cast(ic.HCPCS_CD_40 as string) as hcfa_procedure_code_40
  ,cast(ic.HCPCS_CD_41 as string) as hcfa_procedure_code_41
  ,cast(ic.HCPCS_CD_42 as string) as hcfa_procedure_code_42
  ,cast(ic.HCPCS_CD_43 as string) as hcfa_procedure_code_43
  ,cast(ic.HCPCS_CD_44 as string) as hcfa_procedure_code_44
  ,cast(ic.HCPCS_CD_45 as string) as hcfa_procedure_code_45
  ,ic.update_timestamp as bronze_update_timestamp
  ,current_timestamp as load_timestamp
FROM stream(live.bronze_inpatient_claims) ic

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Outpatient Claims

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_outpatient_claims
AS
SELECT
   oc.DESYNPUF_ID as beneficiary_code
  ,oc.CLM_ID as claim_id
  ,cast(oc.SEGMENT as int) as claim_line_segment
  ,to_date(oc.CLM_FROM_DT,'yyyyMMdd') as claim_start_date
  ,to_date(oc.CLM_THRU_DT,'yyyyMMdd') as claim_end_date
  ,cast(oc.PRVDR_NUM as string) as provider_instituation_code
  ,cast(oc.CLM_PMT_AMT as double) as claim_payment_amount
  ,cast(oc.NCH_PRMRY_PYR_CLM_PD_AMT as double) as primary_payer_claim_paid_amount
  ,cast(oc.AT_PHYSN_NPI as string) as attending_physician_npi
  ,cast(oc.OP_PHYSN_NPI as string) as operating_physician_npi
  ,cast(oc.OT_PHYSN_NPI as string) as other_physician_npi
  ,cast(oc.NCH_BENE_BLOOD_DDCTBL_LBLTY_AM as double) as hch_beneficiary_blood_deductible_liabiliy_limit
  ,cast(oc.ICD9_DGNS_CD_1 as string) as icd9_diagnosis_code_1
  ,cast(oc.ICD9_DGNS_CD_2 as string) as icd9_diagnosis_code_2
  ,cast(oc.ICD9_DGNS_CD_3 as string) as icd9_diagnosis_code_3
  ,cast(oc.ICD9_DGNS_CD_4 as string) as icd9_diagnosis_code_4
  ,cast(oc.ICD9_DGNS_CD_5 as string) as icd9_diagnosis_code_5
  ,cast(oc.ICD9_DGNS_CD_6 as string) as icd9_diagnosis_code_6
  ,cast(oc.ICD9_DGNS_CD_7 as string) as icd9_diagnosis_code_7
  ,cast(oc.ICD9_DGNS_CD_8 as string) as icd9_diagnosis_code_8
  ,cast(oc.ICD9_DGNS_CD_9 as string) as icd9_diagnosis_code_9
  ,cast(oc.ICD9_DGNS_CD_10 as string) as icd9_diagnosis_code_10
  ,cast(oc.ICD9_PRCDR_CD_1 as string) as icd9_procedure_code_1  
  ,cast(oc.ICD9_PRCDR_CD_2 as string) as icd9_procedure_code_2
  ,cast(oc.ICD9_PRCDR_CD_3 as string) as icd9_procedure_code_3
  ,cast(oc.ICD9_PRCDR_CD_4 as string) as icd9_procedure_code_4 
  ,cast(oc.ICD9_PRCDR_CD_5 as string) as icd9_procedure_code_5 
  ,cast(oc.ICD9_PRCDR_CD_6 as string) as icd9_procedure_code_6
  ,cast(oc.NCH_BENE_PTB_DDCTBL_AMT as double) as nch_beneficiary_part_b_deductable_amount
  ,cast(oc.NCH_BENE_PTB_COINSRNC_AMT as double) as nch_beneficiary_part_b_coinsurance_amount
  ,cast(oc.ADMTNG_ICD9_DGNS_CD as string) as icd9_admitting_diagnosis_code
  ,cast(oc.HCPCS_CD_1 as string) as hcfa_procedure_code_1
  ,cast(oc.HCPCS_CD_2 as string) as hcfa_procedure_code_2
  ,cast(oc.HCPCS_CD_3 as string) as hcfa_procedure_code_3
  ,cast(oc.HCPCS_CD_4 as string) as hcfa_procedure_code_4
  ,cast(oc.HCPCS_CD_5 as string) as hcfa_procedure_code_5
  ,cast(oc.HCPCS_CD_6 as string) as hcfa_procedure_code_6
  ,cast(oc.HCPCS_CD_7 as string) as hcfa_procedure_code_7
  ,cast(oc.HCPCS_CD_8 as string) as hcfa_procedure_code_8
  ,cast(oc.HCPCS_CD_9 as string) as hcfa_procedure_code_9
  ,cast(oc.HCPCS_CD_10 as string) as hcfa_procedure_code_10
  ,cast(oc.HCPCS_CD_11 as string) as hcfa_procedure_code_11
  ,cast(oc.HCPCS_CD_12 as string) as hcfa_procedure_code_12
  ,cast(oc.HCPCS_CD_13 as string) as hcfa_procedure_code_13
  ,cast(oc.HCPCS_CD_14 as string) as hcfa_procedure_code_14
  ,cast(oc.HCPCS_CD_15 as string) as hcfa_procedure_code_15
  ,cast(oc.HCPCS_CD_16 as string) as hcfa_procedure_code_16
  ,cast(oc.HCPCS_CD_17 as string) as hcfa_procedure_code_17
  ,cast(oc.HCPCS_CD_18 as string) as hcfa_procedure_code_18
  ,cast(oc.HCPCS_CD_19 as string) as hcfa_procedure_code_19
  ,cast(oc.HCPCS_CD_20 as string) as hcfa_procedure_code_20
  ,cast(oc.HCPCS_CD_21 as string) as hcfa_procedure_code_21
  ,cast(oc.HCPCS_CD_22 as string) as hcfa_procedure_code_22
  ,cast(oc.HCPCS_CD_23 as string) as hcfa_procedure_code_23
  ,cast(oc.HCPCS_CD_24 as string) as hcfa_procedure_code_24
  ,cast(oc.HCPCS_CD_25 as string) as hcfa_procedure_code_25
  ,cast(oc.HCPCS_CD_26 as string) as hcfa_procedure_code_26
  ,cast(oc.HCPCS_CD_27 as string) as hcfa_procedure_code_27
  ,cast(oc.HCPCS_CD_28 as string) as hcfa_procedure_code_28
  ,cast(oc.HCPCS_CD_29 as string) as hcfa_procedure_code_29
  ,cast(oc.HCPCS_CD_30 as string) as hcfa_procedure_code_30
  ,cast(oc.HCPCS_CD_31 as string) as hcfa_procedure_code_31
  ,cast(oc.HCPCS_CD_32 as string) as hcfa_procedure_code_32
  ,cast(oc.HCPCS_CD_33 as string) as hcfa_procedure_code_33
  ,cast(oc.HCPCS_CD_34 as string) as hcfa_procedure_code_34
  ,cast(oc.HCPCS_CD_35 as string) as hcfa_procedure_code_35
  ,cast(oc.HCPCS_CD_36 as string) as hcfa_procedure_code_36
  ,cast(oc.HCPCS_CD_37 as string) as hcfa_procedure_code_37
  ,cast(oc.HCPCS_CD_38 as string) as hcfa_procedure_code_38
  ,cast(oc.HCPCS_CD_39 as string) as hcfa_procedure_code_39
  ,cast(oc.HCPCS_CD_40 as string) as hcfa_procedure_code_40
  ,cast(oc.HCPCS_CD_41 as string) as hcfa_procedure_code_41
  ,cast(oc.HCPCS_CD_42 as string) as hcfa_procedure_code_42
  ,cast(oc.HCPCS_CD_43 as string) as hcfa_procedure_code_43
  ,cast(oc.HCPCS_CD_44 as string) as hcfa_procedure_code_44
  ,cast(oc.HCPCS_CD_45 as string) as hcfa_procedure_code_45
  ,oc.update_timestamp as bronze_update_timestamp
  ,current_timestamp as load_timestamp
FROM stream(live.bronze_outpatient_claims) oc


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Presription Drug Events

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_prescription_drug_events
AS
SELECT
   pde.DESYNPUF_ID as beneficiary_code
  ,cast(pde.PDE_ID as string) as ccw_part_d_event_number
  ,to_date(pde.SRVC_DT,'yyyyMMdd') as rx_service_date
  ,cast(pde.PROD_SRVC_ID as string) as product_service_id
  ,cast(pde.QTY_DSPNSD_NUM as double) as quantity_dispensed
  ,cast(pde.DAYS_SUPLY_NUM as int) as days_supply
  ,cast(pde.PTNT_PAY_AMT as double) as patient_pay_amount
  ,cast(pde.TOT_RX_CST_AMT as double) as gross_drug_cost
  ,pde.update_timestamp as bronze_update_timestamp
  ,current_timestamp as load_timestamp
FROM stream(live.bronze_prescription_drug_events) pde

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###NPI

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_npi_codes
AS
SELECT
   md5(n.npi) as provider_key
  ,cast(n.npi as string) as npi_code
  ,l_entity_type_code.label as entity_type
  ,Replacement_NPI as replacement_npi
  ,Employer_Identification_Number_EIN as employer_identification_number
  ,Provider_Organization_Name_Legal_Business_Name as provider_organization_name
  ,Provider_Last_Name_Legal_Name as provider_last_same
  ,Provider_First_Name as provider_first_name
  ,Provider_Middle_Name as provider_middle_name
  ,Provider_Name_Prefix_Text as provider_name_prefix
  ,Provider_Name_Suffix_Text as provider_name_sufix
  ,Provider_Credential_Text as provider_credential
  ,Provider_Other_Organization_Name as provider_other_organization_name
  ,Provider_Other_Organization_Name_Type_Code as provider_other_organization_name_type_code
  ,Provider_Other_Last_Name as provider_other_last_name
  ,Provider_Other_First_Name as provider_other_first_name
  ,Provider_Other_Middle_Name as provider_other_middle_name
  ,Provider_Other_Name_Prefix_Text as provider_other_name_prefix
  ,Provider_Other_Name_Suffix_Text as provider_other_name_suffix
  ,Provider_Other_Credential_Text as provider_other_credential
  ,Provider_Other_Last_Name_Type_Code as provider_other_last_name_code
  ,Provider_First_Line_Business_Mailing_Address as provider_first_line_business_mailing_address
  ,Provider_Second_Line_Business_Mailing_Address as provider_second_line_business_mailing_address
  ,Provider_Business_Mailing_Address_City_Name as provider_business_mailing_address_city_name
  ,Provider_Business_Mailing_Address_State_Name as provider_business_mailing_address_state_name
  ,Provider_Business_Mailing_Address_Postal_Code as provider_business_mailing_address_postal_code
  ,`Provider_Business_Mailing_Address_Country_Code_If_outside_U.S.` as provider_business_mailing_address_code_if_outside_us
  ,Provider_Business_Mailing_Address_Telephone_Number as provider_business_mailing_address_telephone_number
  ,Provider_Business_Mailing_Address_Fax_Number as provider_business_mailing_address_fax_number
  ,Provider_First_Line_Business_Practice_Location_Address as provider_first_line_business_practice_location
  ,Provider_Second_Line_Business_Practice_Location_Address as provider_second_line_business_practice_location
  ,Provider_Business_Practice_Location_Address_City_Name as provider_business_practice_location_address_city_name
  ,Provider_Business_Practice_Location_Address_State_Name as provider_business_practice_location_address_state_name
  ,Provider_Business_Practice_Location_Address_Postal_Code as provider_business_practice_location_address_postal_code
  ,`Provider_Business_Practice_Location_Address_Country_Code_If_outside_U.S.` as provider_business_practice_location_address_country_code_if_outside_us
  ,Provider_Business_Practice_Location_Address_Telephone_Number as provider_busines_practice_location_address_telephone_number
  ,Provider_Business_Practice_Location_Address_Fax_Number as provider_business_practice_location_address_fax_number
  ,to_date(Provider_Enumeration_Date,'MM/dd/yyyy') as privider_enumeration_date
  ,to_date(Last_Update_Date,'MM/dd/yyyy') as last_update_date
  ,NPI_Deactivation_Reason_Code as npi_deactivation_reason_code
  ,to_date(NPI_Deactivation_Date,'MM/dd/yyyy') as npi_deactivation_date
  ,to_date(NPI_Reactivation_Date,'MM/dd/yyyy') as npi_reactivation_date
  ,l_gender_code.label as provider_gender
  ,Authorized_Official_Last_Name as authorized_official_last_name
  ,Authorized_Official_First_Name as authorized_official_first_name
  ,Authorized_Official_Middle_Name as outhorized_official_middle_name
  ,Authorized_Official_Title_or_Position as authorized_official_title_or_position
  ,Authorized_Official_Telephone_Number as authorized_offiial_telephone_number
  ,Healthcare_Provider_Taxonomy_Code_1 as healthcare_provider_taxonomy_code_1
  ,Provider_License_Number_1 as provider_licence_number_1
  ,Provider_License_Number_State_Code_1 as provider_licene_number_state_code_1
  ,Healthcare_Provider_Primary_Taxonomy_Switch_1 as healthcare_provider_primary_taxonomy_switch_1
  ,Healthcare_Provider_Taxonomy_Code_2 as healthcare_provider_taxonomy_code_2
  ,Provider_License_Number_2 as provider_license_number_2
  ,Provider_License_Number_State_Code_2 as provider_license_number_state_code_2
  ,Healthcare_Provider_Primary_Taxonomy_Switch_2 as healthcare_provider_primary_taxonomy_switch_2
  ,Healthcare_Provider_Taxonomy_Code_3 as healthcare_provider_taxonomy_code_3
  ,Provider_License_Number_3 as provider_license_number_3
  ,Provider_License_Number_State_Code_3 as provider_license_number_state_code_3
  ,Healthcare_Provider_Primary_Taxonomy_Switch_3 as healthcare_provider_primary_taxonomy_switch_3
  ,to_date(Certification_Date,'MM/dd/yyyy') as certification_date
  ,n.update_timestamp as bronze_update_timestamp
  ,current_timestamp as load_timestamp
FROM stream(live.bronze_npi_code) n
left join live.bronze_lookup l_entity_type_code on n.entity_type_code = l_entity_type_code.code and l_entity_type_code.variable = 'entity_type_code'
left join live.bronze_lookup l_gender_code on n.Provider_Gender_Code = l_gender_code.code and l_gender_code.variable = 'gender_code'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###icd codes

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_icd_codes(
  CONSTRAINT `Diagnosis code is not null`    EXPECT (diagnosis_code is not null) ON VIOLATION DROP ROW
)
AS
SELECT
   md5(DiagnosisCode) as diagnosis_key
  ,DiagnosisCode as diagnosis_code
  ,LongDescription as diagnosis_long_description
  ,ShortDescription as diagnosis_short_description
  ,i.update_timestamp as bronze_load_timestamp
  ,current_timestamp as load_timestamp
FROM stream(live.bronze_icd_codes) i
