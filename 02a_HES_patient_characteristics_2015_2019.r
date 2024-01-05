################
# 02a_HES_patient_characteristics_2015_2019.r
# Code to reproduce patient characteristics for calendar years 2015-2019.
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

open_dataset(file.path('APC_final_2019.parquet')) %>% 
  collect() -> APC_final_2019

open_dataset(file.path('APC_final_2018.parquet')) %>% 
  collect() -> APC_final_2018

open_dataset(file.path('APC_final_2017.parquet')) %>% 
  collect() -> APC_final_2017

open_dataset(file.path('APC_final_2016.parquet')) %>% 
  collect() -> APC_final_2016

open_dataset(file.path('APC_final_2015.parquet')) %>% 
  collect() -> APC_final_2015

# Create a list for the relevant years (to be used for loops)
years <- c('2015','2016','2017','2018','2019')



####################################################################
# Calculate the Charlson Score.

#Format the diagnosis and operation codes.
for (year in years) {
  df_new <- get(paste0('APC_final_',year)) %>% 
    mutate_at(vars(contains('DIAG_')), ~ str_replace_all(., '[[:punct:]]', '')) %>% # remove all punctuation from ICD
    mutate_at(vars(contains('DIAG_')), ~ paste0(., 'X')) %>% # add an 'X' place holder to account for ICDs that have fewer than 4 characters (e.g. I10)
    mutate_at(vars(contains('DIAG_')), ~ str_sub(., 1, 4)) %>%  # restrict to 4 chars
    mutate_at(vars(contains('OPERTN_')), ~ str_replace_all(., '[[:punct:]]', '')) %>%     mutate_at(vars(contains('DIAG_')), ~ ifelse(. == 'X', '', .)) # remove all single X
  mutate_at(vars(contains('DIAG_')), ~ ifelse(. == 'X', '', .)) # remove all single X
  
  
  assign(paste0('APC_final_',year), df_new)
}


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
    group_by(spell_id) %>% 
    summarise(SPELDUR = max(SPELDUR_2))
  
  assign(paste0("beddays_",year),beddays)
}

for (year in years) {
  age_group <- get(paste0('APC_final_',year)) %>% 
    group_by(spell_id) %>% 
    summarise(age = max(STARTAGE_corr)) %>% 
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


#Aggregate outputs with two bedday columns (one where 0 stas remain 0, the other where treated as 0.5)
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

age_charlson_2015_2019_all_adm <- left_join(aggregate_output_2015, aggregate_output_2016, by = c('charlson_grouping', 'age_group')) %>% 
  left_join(aggregate_output_2017, by = c('charlson_grouping', 'age_group')) %>% 
  left_join(aggregate_output_2018, by = c('charlson_grouping', 'age_group')) %>%   
  left_join(aggregate_output_2019, by = c('charlson_grouping', 'age_group'))


#Aggregate outputs with 0 stays removed (can calculate LoS)
for (year in years) { 
  
  admission_var <- paste0("admissions_",year)
  bedday_var <- paste0("beddays_",year)
  
  aggregate_output_2 <- get(paste0("output_",year)) %>% 
    filter(SPELDUR>0) %>% 
    group_by(charlson_grouping, age_group) %>% 
    summarise(!!admission_var := n(),
              !!bedday_var := sum(SPELDUR)) %>% 
    ungroup()
  
  assign(paste0("aggregate_output_2_",year),aggregate_output_2)
}



aggregate_output_2_2015 <- aggregate_output_2_2015 %>% 
  mutate(los_2015 = beddays_2015/admissions_2015)
aggregate_output_2_2016 <- aggregate_output_2_2016 %>% 
  mutate(los_2016 = beddays_2016/admissions_2016)
aggregate_output_2_2017 <- aggregate_output_2_2017 %>% 
  mutate(los_2017 = beddays_2017/admissions_2017)
aggregate_output_2_2018 <- aggregate_output_2_2018 %>% 
  mutate(los_2018 = beddays_2018/admissions_2018)
aggregate_output_2_2019 <- aggregate_output_2_2019 %>% 
  mutate(los_2019 = beddays_2019/admissions_2019)


age_charlson_2015_2019 <- left_join(aggregate_output_2_2015, aggregate_output_2_2016, by = c('charlson_grouping', 'age_group')) %>% 
                left_join(aggregate_output_2_2017, by = c('charlson_grouping', 'age_group')) %>% 
                left_join(aggregate_output_2_2018, by = c('charlson_grouping', 'age_group')) %>%   
                left_join(aggregate_output_2_2019, by = c('charlson_grouping', 'age_group'))
