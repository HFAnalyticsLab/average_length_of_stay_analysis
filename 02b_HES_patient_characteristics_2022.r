################
# 02b_HES_patient_characteristics_2022.r
# Code to reproduce patient characteristics for calendar year 2022.
# August 2023
################

# Clear R environment
rm(list = ls())

# Load packages
pacman::p_load(aws.s3, 
               stringr, 
               vroom, 
               dplyr, 
               tidyverse, 
               janitor,
               data.table,
               dtplyr,
               arrow,
               comorbidity)

####################################################################
# Import the relevant datasets to the environment.

open_dataset(file.path('APC_final_2022.parquet')) %>% 
  collect() -> APC_final_2022

# Create a list for the relevant years (to be used for loops)
years <- c('2022')

####################################################################
#Create a Covid primary and secondary flag to the dataset.

#Function to flag particular diagnoses.
id_str <- function(data, find, cols = 'DIAG_01', count = FALSE){
  data <- data %>% select(starts_with(cols))
  if(count){ # count times
    apply(data, 1, function(t) {
      sum(grepl(pattern = find, x = t))
    })
  } else { # flag
    apply(data, 1, function(t) {
      ifelse(sum(grepl(pattern = find, x = t)) > 0, 1, 0)
    })  
  }
}

#Add diagnosis flag to data
APC_final_2022$covid_primary_flag <- id_str(APC_final_2022, 'U071|U072', count = TRUE)


#Function to flag particular diagnoses.
id_str <- function(data, find, cols = 'DIAG_', count = FALSE){
  data <- data %>% select(starts_with(cols))
  if(count){ # count times
    apply(data, 1, function(t) {
      sum(grepl(pattern = find, x = t))
    })
  } else { # flag
    apply(data, 1, function(t) {
      ifelse(sum(grepl(pattern = find, x = t)) > 0, 1, 0)
    })  
  }
}


#Add diagnosis flag to data
APC_final_2022$covid_secondary_flag <- id_str(APC_final_2022, 'U071|U072', count = TRUE)

APC_final_2022 <- APC_final_2022 %>%
  mutate(covid_secondary_flag=ifelse(covid_secondary_flag==1 & covid_primary_flag==1,
                                     0,covid_secondary_flag))

APC_final_2022 <- APC_final_2022 %>% 
  group_by(spell_id) %>% 
  mutate(covid_first = max(covid_primary_flag),
         covid_second = max(covid_secondary_flag)) %>% 
  ungroup() %>% 
  mutate(covid_flag=ifelse(covid_first==1,1,
                           ifelse(covid_second==1,2,0))) %>% 
  select(-covid_first,-covid_second)


####################################################################
# Charlson Score and Age banding.

#Format the diagnosis and operation codes.
for (year in years) {
  df_new <- get(paste0('APC_final_',year)) %>% 
    mutate_at(vars(contains('DIAG_')), ~ str_replace_all(., '[[:punct:]]', '')) %>% # remove all punctuation from ICD
    mutate_at(vars(contains('DIAG_')), ~ paste0(., 'X')) %>% # add an 'X' place holder to account for ICDs that have fewer than 4 characters (e.g. I10)
    mutate_at(vars(contains('DIAG_')), ~ str_sub(., 1, 4)) %>%  # restrict to 4 chars
    mutate_at(vars(contains('OPERTN_')), ~ str_replace_all(., '[[:punct:]]', '')) %>%
    mutate_at(vars(contains('DIAG_')), ~ ifelse(. == 'X', '', .)) # remove all single X
  
  assign(paste0('APC_final_',year), df_new)
}


APC_final_2022 <- APC_final_2022 %>% 
  mutate_at(vars(contains('DIAG_')), ~ ifelse(. == 'NAXX', '', .))

#Format the diagnosis codes to have one per row.
for (year in years) {
  comorbid_format <- get(paste0("APC_final_",year)) %>%
    select(spell_id,starts_with('DIAG_')) %>% 
    pivot_longer(cols = starts_with("DIAG_"),
                 names_to = "diag_col",
                 values_to = "diag") %>% 
    select(spell_id,diag)
  
  assign(paste0("comorbid_format_",year), comorbid_format)
}

#Calculate the Charlson comorbidity index for each spell.
for (year in years) { 
  charlson <- comorbidity(get(paste0("comorbid_format_",year)),id="spell_id", code="diag", map="charlson_icd10_quan", assign0=FALSE)
  
  assign(paste0("charlson_",year), charlson)
}

#Calculate the Charlson comorbidity index for each spell.
for (year in years) { 
  score <- score(get(paste0("charlson_",year)), weights = NULL, assign0=FALSE) %>% 
    as.data.frame() %>% 
    rename(score = '.')
  
  assign(paste0("score_",year),score)
}

#Group comorbidity scores into 4 buckets.
for (year in years) { 
  group <- get(paste0("score_",year)) %>% 
    mutate(charlson_grouping = case_when(
      score == 0 ~ "None",
      score %in% 1:2 ~ "Mild",
      score %in% 3:4 ~ "Moderate",
      score >= 5 ~ "Severe"
    )) %>% 
    mutate(spell_id = row_number())
  
  assign(paste0("group_",year),group)
}

#Calculate the beddays for each stay.
for (year in years) { 
  beddays <- get(paste0("APC_final_",year)) %>% 
    group_by(spell_id,covid_flag) %>% 
    summarise(SPELDUR = max(SPELDUR_2))
  
  assign(paste0("beddays_",year),beddays)
}

#Calculate the age and IMD for each stay.
for (year in years) {
  age_group <- get(paste0('APC_final_',year)) %>% 
    group_by(spell_id) %>% 
    summarise(age = max(STARTAGE_corr),
              IMDquin = first(IMDquin)) %>% 
    mutate(age_group = case_when(
      age >= 18 & age < 45 ~ "18-44",
      age >= 45 & age < 65 ~ "45-64",
      age >= 65 & age < 75 ~ "65-74",
      age >= 75 & age < 85 ~ "75-84",
      age >= 85 ~ "85+")) %>%
    select(-age) %>% 
    ungroup()
  
  assign(paste0('age_group_',year), age_group)
}

#Bring together the stays and groupings
for (year in years) { 
  output1 <- merge(get(paste0("group_",year)),get(paste0("beddays_",year)), by = 'spell_id')
  
  output2 <- merge(output1,get(paste0("age_group_",year)), by = 'spell_id')
  
  assign(paste0("output_",year),output2)
}


#Aggregate outputs by charlson score and age banding.
for (year in years) { 
  
  admission_var <- paste0("admissions_",year)
  bedday_var <- paste0("beddays_",year)  
  
  aggregate_output <- get(paste0("output_",year)) %>% 
    group_by(charlson_grouping, age_group) %>% 
    summarise(!!admission_var := n(),
              !!bedday_var := sum(SPELDUR)) %>% 
    ungroup()
  
    assign(paste0("aggregate_output_",year),aggregate_output)
}

age_charlson_2022_all_adm <- aggregate_output_2022

write_parquet(age_charlson_2022_all_adm,
              'age_charlson_2022_all_adm.parquet')


#Aggregate outputs with 0 stays removed (can calculate LoS)
for (year in years) { 
  
  admission_var <- paste0("admissions_",year)
  bedday_var <- paste0("beddays_",year)
  
  aggregate_output_2 <- get(paste0("output_",year)) %>% 
    filter(SPELDUR>0) %>% 
    group_by(charlson_grouping, age_group, covid_flag) %>% 
    summarise(!!admission_var := n(),
              !!bedday_var := sum(SPELDUR)) %>% 
    ungroup()
  
  assign(paste0("aggregate_output_2_",year),aggregate_output_2)
}


age_charlson_covid_2022 <- aggregate_output_2_2022

write_parquet(age_charlson_covid_2022,
              'age_charlson_covid_2022.parquet')


#Select aggregated file with no covid variables.
age_charlson_2022 <- aggregate_output_2_2022 %>% 
  group_by(charlson_grouping, age_group) %>% 
  summarise(admissions_2022 = sum(admissions_2022),
            beddays_2022 = sum(beddays_2022)) %>% 
  mutate(los_2022 = beddays_2022/admissions_2022) %>% 
  ungroup()

write_parquet(age_charlson_2022,
              'age_charlson_2022.parquet')
