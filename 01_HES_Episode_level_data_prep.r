################
# 01_HES_Episode_level_data_prep.r
# Code to reproduce construction of episode level HES data for calendar years
# 2015-2022 (excluding 2020-21)
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
#Create a calendar year 2022 file.
#Need to extract out Apr-Dec from 2022 data file and Jan-Mar from 2021 data files.


#Map to the parquet file.

year <- '2022'

open_dataset(file.path(APC_location, year)) %>%
  select(TOKEN_PERSON_ID,
         PROVSPNOPS,
         ADMIDATE,
         EPISTART,
         EPIEND,
         DISDATE,
         EPISTAT,
         CLASSPAT,
         SPELDUR,
         ETHNOS,
         STARTAGE,
         SEX,
         LSOA11,
         PROCODE,
         PROCODET,
         SUSRECID,
         EPIORDER,
         ADMISORC,
         ADMIMETH,
         DISDEST,
         DISMETH,
         EPIDUR,
         MAINSPEF,
         NEOCARE,
         TRETSPEF,
         #    CRITICALCAREDAYS,
         #    REHABILITATIONDAYS,
         #    SPCDAYS,
         starts_with('DIAG_'),
         starts_with('OPERTN_')
  ) %>%
  mutate(DISDATE=as.Date(DISDATE, "%Y-%m-%d"), EPISTART=as.Date(EPISTART, "%Y-%m-%d"),
         ADMIDATE=as.Date(ADMIDATE, "%Y-%m-%d"),EPIEND=as.Date(EPIEND, "%Y-%m-%d"),
         ADMICAT = as.numeric(str_sub(ADMIMETH,1,1))) %>% #Create a variable which identifies if elective or non-elective.
  filter(EPISTAT == 3 & !is.na(ADMIDATE) & CLASSPAT==1 & ADMICAT==2) %>% #Filter for closed episodes, those with completed admission date and non-elective only.
  collect() -> APC_data_2022


#Select out spells which take place within the time frame
APC_data_2022 <- APC_data_2022 %>% 
  group_by(TOKEN_PERSON_ID, PROVSPNOPS) %>% 
  mutate(DISDATE_2 = max(DISDATE, na.rm=T)) %>% 
  filter(DISDATE_2 < as.Date("2023-01-01") & DISDATE_2 >= as.Date("2022-04-01")) %>% 
  ungroup()


#Define function to calculate mode
find_mode <- function(x) {
  u <- unique(na.omit(x))
  u[which.max(tabulate(match(x, u)))]
}
#Create variable with mode of LSOA within an admission
APC_data_2022 <- APC_data_2022 %>%
  group_by(TOKEN_PERSON_ID, ADMIDATE) %>%
  mutate(LSOA_corr = find_mode(LSOA11)) %>%
  select(-LSOA11) %>% 
  ungroup()

#Create variable with mode of start age.
APC_data_2022 <- APC_data_2022 %>%
  group_by(TOKEN_PERSON_ID, ADMIDATE) %>%
  mutate(STARTAGE_corr = find_mode(STARTAGE)) %>%
  select(-STARTAGE)%>%
  ungroup()

#Select out adults only.
APC_data_2022 <- APC_data_2022 %>% 
  filter(STARTAGE_corr >= 18 & STARTAGE_corr < 130)

####################################################################
#Create a 2021 data file.


year <- '2021'

open_dataset(file.path(APC_location, year)) %>% ## grouper asks for PROVSPNO, we only have PROVSPNOPS?
  select(TOKEN_PERSON_ID,
         PROVSPNOPS,
         ADMIDATE,
         EPISTART,
         EPIEND,
         DISDATE,
         EPISTAT,
         CLASSPAT,
         SPELDUR,
         ETHNOS,
         STARTAGE,
         SEX,
         LSOA11,
         PROCODE,
         PROCODET,
         SUSRECID,
         EPIORDER,
         ADMISORC,
         ADMIMETH,
         DISDEST,
         DISMETH,
         EPIDUR,
         MAINSPEF,
         NEOCARE,
         TRETSPEF,
         #    CRITICALCAREDAYS,
         #    REHABILITATIONDAYS,
         #    SPCDAYS,
         starts_with('DIAG_'),
         starts_with('OPERTN_')
  ) %>%
  mutate(DISDATE=as.Date(DISDATE, "%Y-%m-%d"), EPISTART=as.Date(EPISTART, "%Y-%m-%d"),
         ADMIDATE=as.Date(ADMIDATE, "%Y-%m-%d"),EPIEND=as.Date(EPIEND, "%Y-%m-%d"),
         ADMICAT = as.numeric(str_sub(ADMIMETH,1,1))) %>% 
  filter(EPISTAT == 3 & !is.na(ADMIDATE) & CLASSPAT==1 & ADMICAT==2) %>% 
  collect() -> APC_data_2021


#Select out spells which take place within the time frame
APC_data_2021 <- APC_data_2021 %>% 
  group_by(TOKEN_PERSON_ID, PROVSPNOPS) %>% 
  mutate(DISDATE_2 = max(DISDATE, na.rm=T)) %>% 
  filter(DISDATE_2 < as.Date("2022-04-01") & DISDATE_2 >= as.Date("2022-01-01")) %>% 
  ungroup()


#Define function to calculate mode
find_mode <- function(x) {
  u <- unique(na.omit(x))
  u[which.max(tabulate(match(x, u)))]
}
#Create variable with mode of LSOA within an admission
APC_data_2021 <- APC_data_2021 %>%
  group_by(TOKEN_PERSON_ID, ADMIDATE) %>%
  mutate(LSOA_corr = find_mode(LSOA11)) %>%
  select(-LSOA11) %>% 
  ungroup()

#Create variable with mode of start age.
APC_data_2021 <- APC_data_2021 %>%
  group_by(TOKEN_PERSON_ID, ADMIDATE) %>%
  mutate(STARTAGE_corr = find_mode(STARTAGE)) %>%
  select(-STARTAGE)%>%
  ungroup()

#Select out adults only.
APC_data_2021 <- APC_data_2021 %>% 
  filter(STARTAGE_corr >= 18 & STARTAGE_corr < 130)

APC_data_2021 <- as.data.frame(APC_data_2021)

####################################################################
#Bring together the separate 2021 and 2022 data files.

APC_final_2022 <- bind_rows(APC_data_2021,APC_data_2022)

#Flag total speldur across all records in spell.
APC_final_2022 <- APC_final_2022 %>% 
  group_by(TOKEN_PERSON_ID, PROVSPNOPS) %>% 
  mutate(SPELDUR_2 = max(SPELDUR, na.rm=T)) %>%
  ungroup()

#Some spells have NA speldur. In these instances, calculate the duration from admission to discharge.
APC_final_2022 <- APC_final_2022 %>% 
  mutate(SPELDUR_2=ifelse(SPELDUR_2 < 0,DISDATE_2 - ADMIDATE,SPELDUR_2))

#Identify duplicate records and remove.
APC_final_2022 <- APC_final_2022 %>% 
  group_by_at(vars(TOKEN_PERSON_ID, TRETSPEF, EPISTART, EPIEND, ADMIDATE, SPELDUR_2)) %>%
  mutate(Flag = ifelse(n() > 1,
                       1,
                       0))

tabyl(as.data.frame(APC_final_2022)$Flag)

#Remove spells which began more than 5 years ago and over 5 years long.
APC_final_2022 <- APC_final_2022 %>% 
  filter(ADMIDATE >= as.Date("2017-01-01") & SPELDUR_2 < 1826)

#Create a unique, and numeric, spell id for each stay. Some provspnops are identical for different individuals.
APC_final_2022 <- APC_final_2022 %>% 
  filter(Flag==0) %>% 
  group_by(TOKEN_PERSON_ID,PROVSPNOPS) %>% 
  mutate(spell_id=cur_group_id()) %>%
  ungroup()

max(APC_final_2022$spell_id)

write_parquet(APC_final_2022,
              'APC_final_2022.parquet')


####################################################################
#Create a 2019 data file.


year <- '2019'

open_dataset(file.path(APC_location, year)) %>% ## grouper asks for PROVSPNO, we only have PROVSPNOPS?
  select(ENCRYPTED_HESID,
         PROVSPNOPS,
         ADMIDATE,
         EPISTART,
         EPIEND,
         DISDATE,
         EPISTAT,
         CLASSPAT,
         SPELDUR,
         ETHNOS,
         STARTAGE,
         SEX,
         LSOA11,
         PROCODE,
         PROCODET,
         SUSRECID,
         EPIORDER,
         ADMISORC,
         ADMIMETH,
         DISDEST,
         DISMETH,
         EPIDUR,
         MAINSPEF,
         NEOCARE,
         TRETSPEF,
         #    CRITICALCAREDAYS,
         #    REHABILITATIONDAYS,
         #    SPCDAYS,
         starts_with('DIAG_'),
         starts_with('OPERTN_')
  ) %>%
  mutate(DISDATE=as.Date(DISDATE, "%Y-%m-%d"), EPISTART=as.Date(EPISTART, "%Y-%m-%d"),
         ADMIDATE=as.Date(ADMIDATE, "%Y-%m-%d"),EPIEND=as.Date(EPIEND, "%Y-%m-%d"),
         ADMICAT = as.numeric(str_sub(ADMIMETH,1,1))) %>% 
  filter(EPISTAT == 3 & !is.na(ADMIDATE) & CLASSPAT==1 & ADMICAT==2) %>% 
  collect() -> APC_data_2019


#Select out spells which take place within the time frame
APC_data_2019 <- APC_data_2019 %>% 
  group_by(ENCRYPTED_HESID, PROVSPNOPS) %>% 
  mutate(DISDATE_2 = max(DISDATE, na.rm=T)) %>% 
  filter(DISDATE_2 < as.Date("2020-01-01") & DISDATE_2 >= as.Date("2019-04-01")) %>% 
  ungroup()


#Define function to calculate mode
find_mode <- function(x) {
  u <- unique(na.omit(x))
  u[which.max(tabulate(match(x, u)))]
}
#Create variable with mode of LSOA within an admission
APC_data_2019 <- APC_data_2019 %>%
  group_by(ENCRYPTED_HESID, ADMIDATE) %>%
  mutate(LSOA_corr = find_mode(LSOA11)) %>%
  select(-LSOA11) %>% 
  ungroup()

#Create variable with mode of start age.
APC_data_2019 <- APC_data_2019 %>%
  group_by(ENCRYPTED_HESID, ADMIDATE) %>%
  mutate(STARTAGE_corr = find_mode(STARTAGE)) %>%
  select(-STARTAGE)%>%
  ungroup()

#Select out adults only.
APC_data_2019 <- APC_data_2019 %>% 
  filter(STARTAGE_corr >= 18 & STARTAGE_corr < 130)

APC_data_2019 <- as.data.frame(APC_data_2019)

####################################################################
#Create a 2018 data file.


year <- '2018'

open_dataset(file.path(APC_location, year)) %>% ## grouper asks for PROVSPNO, we only have PROVSPNOPS?
  select(ENCRYPTED_HESID,
         PROVSPNOPS,
         ADMIDATE,
         EPISTART,
         EPIEND,
         DISDATE,
         EPISTAT,
         CLASSPAT,
         SPELDUR,
         ETHNOS,
         STARTAGE,
         SEX,
         LSOA11,
         PROCODE,
         PROCODET,
         SUSRECID,
         EPIORDER,
         STARTAGE,
         SEX,
         CLASSPAT,
         ADMISORC,
         ADMIMETH,
         DISDEST,
         DISMETH,
         EPIDUR,
         MAINSPEF,
         NEOCARE,
         TRETSPEF,
         #    CRITICALCAREDAYS,
         #    REHABILITATIONDAYS,
         #    SPCDAYS,
         starts_with('DIAG_'),
         starts_with('OPERTN_')
  ) %>%
  mutate(DISDATE=as.Date(DISDATE, "%Y-%m-%d"), EPISTART=as.Date(EPISTART, "%Y-%m-%d"),
         ADMIDATE=as.Date(ADMIDATE, "%Y-%m-%d"),EPIEND=as.Date(EPIEND, "%Y-%m-%d"),
         ADMICAT = as.numeric(str_sub(ADMIMETH,1,1))) %>% 
  filter(EPISTAT == 3 & !is.na(ADMIDATE) & CLASSPAT==1 & ADMICAT==2) %>% 
  collect() -> APC_data_2018


#Select out spells which take place within the time frame
APC_data_2018 <- APC_data_2018 %>% 
  group_by(ENCRYPTED_HESID, PROVSPNOPS) %>% 
  mutate(DISDATE_2 = max(DISDATE, na.rm=T)) %>% 
  filter(DISDATE_2 < as.Date("2019-04-01") & DISDATE_2 >= as.Date("2019-01-01")) %>% 
  ungroup()


#Define function to calculate mode
find_mode <- function(x) {
  u <- unique(na.omit(x))
  u[which.max(tabulate(match(x, u)))]
}
#Create variable with mode of LSOA within an admission
APC_data_2018 <- APC_data_2018 %>%
  group_by(ENCRYPTED_HESID, ADMIDATE) %>%
  mutate(LSOA_corr = find_mode(LSOA11)) %>%
  select(-LSOA11) %>% 
  ungroup()

#Create variable with mode of start age.
APC_data_2018 <- APC_data_2018 %>%
  group_by(ENCRYPTED_HESID, ADMIDATE) %>%
  mutate(STARTAGE_corr = find_mode(STARTAGE)) %>%
  select(-STARTAGE)%>%
  ungroup()

#Select out adults only.
APC_data_2018 <- APC_data_2018 %>% 
  filter(STARTAGE_corr >= 18 & STARTAGE_corr < 130)

APC_data_2018 <- as.data.frame(APC_data_2018)

####################################################################
#Bring together the separate 2018 and 2019 data files.

APC_final_2019 <- bind_rows(APC_data_2018,APC_data_2019)

#Flag total speldur across all records in spell.
APC_final_2019 <- APC_final_2019 %>% 
  group_by(ENCRYPTED_HESID, PROVSPNOPS) %>% 
  mutate(SPELDUR_2 = max(SPELDUR, na.rm=T)) %>%
  ungroup()

#Some spells have null speldur. In these instances, calculate the duration from admission to discharge.
APC_final_2019 <- APC_final_2019 %>% 
  mutate(SPELDUR_2=ifelse(SPELDUR_2 < 0,DISDATE_2 - ADMIDATE,SPELDUR_2))

#Create a unique spell id (PROVSPNOPS sometimes the same for different patients)
APC_final_2019 <- APC_final_2019 %>% 
  group_by(ENCRYPTED_HESID,PROVSPNOPS) %>% 
  mutate(spell_id=cur_group_id()) %>%
  ungroup()

max(APC_final_2019$spell_id)


#Identify duplicate records and remove.
APC_final_2019 <- APC_final_2019 %>% 
  group_by_at(vars(ENCRYPTED_HESID, TRETSPEF, EPISTART, EPIEND, ADMIDATE, SPELDUR_2)) %>%
  mutate(Flag = ifelse(n() > 1,
                       1,
                       0))

tabyl(as.data.frame(APC_final_2019)$Flag)

#Remove spells which began more than 5 years ago and over 5 years long.
APC_final_2019 <- APC_final_2019 %>% 
  filter(ADMIDATE >= as.Date("2014-01-01") & SPELDUR_2 < 1826)

#Create a unique, and numeric, spell id for each stay. Some provspnops are identical for different individuals.
APC_final_2019 <- APC_final_2019 %>% 
  filter(Flag==0) %>% 
  group_by(ENCRYPTED_HESID,PROVSPNOPS) %>% 
  mutate(spell_id=cur_group_id()) %>%
  ungroup()

max(APC_final_2019$spell_id)

write_parquet(APC_final_2019,
              'APC_final_2019.parquet')



####################################################################
#Create a 2018 data file.


year <- '2018'

open_dataset(file.path(APC_location, year)) %>% ## grouper asks for PROVSPNO, we only have PROVSPNOPS?
  select(ENCRYPTED_HESID,
         PROVSPNOPS,
         ADMIDATE,
         EPISTART,
         EPIEND,
         DISDATE,
         EPISTAT,
         CLASSPAT,
         SPELDUR,
         ETHNOS,
         STARTAGE,
         SEX,
         LSOA11,
         PROCODE,
         PROCODET,
         SUSRECID,
         EPIORDER,
         ADMISORC,
         ADMIMETH,
         DISDEST,
         DISMETH,
         EPIDUR,
         MAINSPEF,
         NEOCARE,
         TRETSPEF,
         #    CRITICALCAREDAYS,
         #    REHABILITATIONDAYS,
         #    SPCDAYS,
         starts_with('DIAG_'),
         starts_with('OPERTN_')
  ) %>%
  mutate(DISDATE=as.Date(DISDATE, "%Y-%m-%d"), EPISTART=as.Date(EPISTART, "%Y-%m-%d"),
         ADMIDATE=as.Date(ADMIDATE, "%Y-%m-%d"),EPIEND=as.Date(EPIEND, "%Y-%m-%d"),
         ADMICAT = as.numeric(str_sub(ADMIMETH,1,1))) %>% #Create a variable which identifies if elective or non-elective.
  filter(EPISTAT == 3 & !is.na(ADMIDATE) & CLASSPAT==1 & ADMICAT==2) %>% 
  collect() -> APC_data_2018


#Select out spells which take place within the time frame
APC_data_2018 <- APC_data_2018 %>% 
  group_by(ENCRYPTED_HESID, PROVSPNOPS) %>% 
  mutate(DISDATE_2 = max(DISDATE, na.rm=T)) %>% 
  filter(DISDATE_2 < as.Date("2019-01-01") & DISDATE_2 >= as.Date("2018-04-01")) %>% 
  ungroup()


#Define function to calculate mode
find_mode <- function(x) {
  u <- unique(na.omit(x))
  u[which.max(tabulate(match(x, u)))]
}
#Create variable with mode of LSOA within an admission
APC_data_2018 <- APC_data_2018 %>%
  group_by(ENCRYPTED_HESID, ADMIDATE) %>%
  mutate(LSOA_corr = find_mode(LSOA11)) %>%
  select(-LSOA11) %>% 
  ungroup()

#Create variable with mode of start age.
APC_data_2018 <- APC_data_2018 %>%
  group_by(ENCRYPTED_HESID, ADMIDATE) %>%
  mutate(STARTAGE_corr = find_mode(STARTAGE)) %>%
  select(-STARTAGE)%>%
  ungroup()

#Select out adults only.
APC_data_2018 <- APC_data_2018 %>% 
  filter(STARTAGE_corr >= 18 & STARTAGE_corr < 130)

APC_data_2018 <- as.data.frame(APC_data_2018)

####################################################################
#Create a 2017 data file.


year <- '2017'

open_dataset(file.path(APC_location, year)) %>% ## grouper asks for PROVSPNO, we only have PROVSPNOPS?
  select(ENCRYPTED_HESID,
         PROVSPNOPS,
         ADMIDATE,
         EPISTART,
         EPIEND,
         DISDATE,
         EPISTAT,
         CLASSPAT,
         SPELDUR,
         ETHNOS,
         STARTAGE,
         SEX,
         LSOA11,
         PROCODE,
         PROCODET,
         SUSRECID,
         EPIORDER,
         ADMISORC,
         ADMIMETH,
         DISDEST,
         DISMETH,
         EPIDUR,
         MAINSPEF,
         NEOCARE,
         TRETSPEF,
         #    CRITICALCAREDAYS,
         #    REHABILITATIONDAYS,
         #    SPCDAYS,
         starts_with('DIAG_'),
         starts_with('OPERTN_')
  ) %>%
  mutate(DISDATE=as.Date(DISDATE, "%Y-%m-%d"), EPISTART=as.Date(EPISTART, "%Y-%m-%d"),
         ADMIDATE=as.Date(ADMIDATE, "%Y-%m-%d"),EPIEND=as.Date(EPIEND, "%Y-%m-%d"),
         ADMICAT = as.numeric(str_sub(ADMIMETH,1,1))) %>% 
  filter(EPISTAT == 3 & !is.na(ADMIDATE) & CLASSPAT==1 & ADMICAT==2) %>% 
  collect() -> APC_data_2017


#Select out spells which take place within the time frame
APC_data_2017 <- APC_data_2017 %>% 
  group_by(ENCRYPTED_HESID, PROVSPNOPS) %>% 
  mutate(DISDATE_2 = max(DISDATE, na.rm=T)) %>% 
  filter(DISDATE_2 < as.Date("2018-04-01") & DISDATE_2 >= as.Date("2018-01-01")) %>% 
  ungroup()


#Define function to calculate mode
find_mode <- function(x) {
  u <- unique(na.omit(x))
  u[which.max(tabulate(match(x, u)))]
}
#Create variable with mode of LSOA within an admission
APC_data_2017 <- APC_data_2017 %>%
  group_by(ENCRYPTED_HESID, ADMIDATE) %>%
  mutate(LSOA_corr = find_mode(LSOA11)) %>%
  select(-LSOA11) %>% 
  ungroup()

#Create variable with mode of start age.
APC_data_2017 <- APC_data_2017 %>%
  group_by(ENCRYPTED_HESID, ADMIDATE) %>%
  mutate(STARTAGE_corr = find_mode(STARTAGE)) %>%
  select(-STARTAGE)%>%
  ungroup()

#Select out adults only.
APC_data_2017 <- APC_data_2017 %>% 
  filter(STARTAGE_corr >= 18 & STARTAGE_corr < 130)

APC_data_2017 <- as.data.frame(APC_data_2017)

####################################################################
#Bring together the separate 2017 and 2018 data files.

APC_final_2018 <- bind_rows(APC_data_2017,APC_data_2018)

#Flag total speldur across all records in spell.
APC_final_2018 <- APC_final_2018 %>% 
  group_by(ENCRYPTED_HESID, PROVSPNOPS) %>% 
  mutate(SPELDUR_2 = max(SPELDUR, na.rm=T)) %>%
  ungroup()

#Some spells have NA speldur. In these instances, calculate the duration from admission to discharge.
APC_final_2018 <- APC_final_2018 %>% 
  mutate(SPELDUR_2=ifelse(SPELDUR_2 < 0,DISDATE_2 - ADMIDATE,SPELDUR_2))

#Identify duplicate records and remove.
APC_final_2018 <- APC_final_2018 %>% 
  group_by_at(vars(ENCRYPTED_HESID, TRETSPEF, EPISTART, EPIEND, ADMIDATE, SPELDUR_2)) %>%
  mutate(Flag = ifelse(n() > 1,
                       1,
                       0))

tabyl(as.data.frame(APC_final_2018)$Flag)

#Remove spells which began more than 5 years ago and over 5 years long.
APC_final_2018 <- APC_final_2018 %>% 
  filter(ADMIDATE >= as.Date("2013-01-01") & SPELDUR_2 < 1826)

#Create a unique, and numeric, spell id for each stay. Some provspnops are identical for different individuals.
APC_final_2018 <- APC_final_2018 %>% 
  filter(Flag==0) %>% 
  group_by(ENCRYPTED_HESID,PROVSPNOPS) %>% 
  mutate(spell_id=cur_group_id()) %>%
  ungroup()

write_parquet(APC_final_2018,
              'APC_final_2018.parquet')


####################################################################
#Create a 2017 data file.


year <- '2017'

open_dataset(file.path(APC_location, year)) %>% ## grouper asks for PROVSPNO, we only have PROVSPNOPS?
  select(ENCRYPTED_HESID,
         PROVSPNOPS,
         ADMIDATE,
         EPISTART,
         EPIEND,
         DISDATE,
         EPISTAT,
         CLASSPAT,
         SPELDUR,
         ETHNOS,
         STARTAGE,
         SEX,
         LSOA11,
         PROCODE,
         PROCODET,
         SUSRECID,
         EPIORDER,
         ADMISORC,
         ADMIMETH,
         DISDEST,
         DISMETH,
         EPIDUR,
         MAINSPEF,
         NEOCARE,
         TRETSPEF,
         #    CRITICALCAREDAYS,
         #    REHABILITATIONDAYS,
         #    SPCDAYS,
         starts_with('DIAG_'),
         starts_with('OPERTN_')
  ) %>%
  mutate(DISDATE=as.Date(DISDATE, "%Y-%m-%d"), EPISTART=as.Date(EPISTART, "%Y-%m-%d"),
         ADMIDATE=as.Date(ADMIDATE, "%Y-%m-%d"),EPIEND=as.Date(EPIEND, "%Y-%m-%d"),
         ADMICAT = as.numeric(str_sub(ADMIMETH,1,1))) %>% #Create a variable which identifies if elective or non-elective.
  filter(EPISTAT == 3 & !is.na(ADMIDATE) & CLASSPAT==1 & ADMICAT==2) %>% 
  collect() -> APC_data_2017


#Select out spells which take place within the time frame
APC_data_2017 <- APC_data_2017 %>% 
  group_by(ENCRYPTED_HESID, PROVSPNOPS) %>% 
  mutate(DISDATE_2 = max(DISDATE, na.rm=T)) %>% 
  filter(DISDATE_2 < as.Date("2018-01-01") & DISDATE_2 >= as.Date("2017-04-01")) %>% 
  ungroup()


#Define function to calculate mode
find_mode <- function(x) {
  u <- unique(na.omit(x))
  u[which.max(tabulate(match(x, u)))]
}
#Create variable with mode of LSOA within an admission
APC_data_2017 <- APC_data_2017 %>%
  group_by(ENCRYPTED_HESID, ADMIDATE) %>%
  mutate(LSOA_corr = find_mode(LSOA11)) %>%
  select(-LSOA11) %>% 
  ungroup()

#Create variable with mode of start age.
APC_data_2017 <- APC_data_2017 %>%
  group_by(ENCRYPTED_HESID, ADMIDATE) %>%
  mutate(STARTAGE_corr = find_mode(STARTAGE)) %>%
  select(-STARTAGE)%>%
  ungroup()

#Select out adults only.
APC_data_2017 <- APC_data_2017 %>% 
  filter(STARTAGE_corr >= 18 & STARTAGE_corr < 130)

APC_data_2017 <- as.data.frame(APC_data_2017)


####################################################################
#Create a 2016 data file.


year <- '2016'

open_dataset(file.path(APC_location, year)) %>% ## grouper asks for PROVSPNO, we only have PROVSPNOPS?
  select(ENCRYPTED_HESID,
         PROVSPNOPS,
         ADMIDATE,
         EPISTART,
         EPIEND,
         DISDATE,
         EPISTAT,
         CLASSPAT,
         SPELDUR,
         ETHNOS,
         STARTAGE,
         SEX,
         LSOA11,
         PROCODE,
         PROCODET,
         SUSRECID,
         EPIORDER,
         ADMISORC,
         ADMIMETH,
         DISDEST,
         DISMETH,
         EPIDUR,
         MAINSPEF,
         NEOCARE,
         TRETSPEF,
         #    CRITICALCAREDAYS,
         #    REHABILITATIONDAYS,
         #    SPCDAYS,
         starts_with('DIAG_'),
         starts_with('OPERTN_')
  ) %>%
  mutate(DISDATE=as.Date(DISDATE, "%Y-%m-%d"), EPISTART=as.Date(EPISTART, "%Y-%m-%d"),
         ADMIDATE=as.Date(ADMIDATE, "%Y-%m-%d"),EPIEND=as.Date(EPIEND, "%Y-%m-%d"),
         ADMICAT = as.numeric(str_sub(ADMIMETH,1,1))) %>% 
  filter(EPISTAT == 3 & !is.na(ADMIDATE) & CLASSPAT==1 & ADMICAT==2) %>% 
  collect() -> APC_data_2016


#Select out spells which take place within the time frame
APC_data_2016 <- APC_data_2016 %>% 
  group_by(ENCRYPTED_HESID, PROVSPNOPS) %>% 
  mutate(DISDATE_2 = max(DISDATE, na.rm=T)) %>% 
  filter(DISDATE_2 < as.Date("2017-04-01") & DISDATE_2 >= as.Date("2017-01-01")) %>% 
  ungroup()


#Define function to calculate mode
find_mode <- function(x) {
  u <- unique(na.omit(x))
  u[which.max(tabulate(match(x, u)))]
}
#Create variable with mode of LSOA within an admission
APC_data_2016 <- APC_data_2016 %>%
  group_by(ENCRYPTED_HESID, ADMIDATE) %>%
  mutate(LSOA_corr = find_mode(LSOA11)) %>%
  select(-LSOA11) %>% 
  ungroup()

#Create variable with mode of start age.
APC_data_2016 <- APC_data_2016 %>%
  group_by(ENCRYPTED_HESID, ADMIDATE) %>%
  mutate(STARTAGE_corr = find_mode(STARTAGE)) %>%
  select(-STARTAGE)%>%
  ungroup()

#Select out adults only.
APC_data_2016 <- APC_data_2016 %>% 
  filter(STARTAGE_corr >= 18 & STARTAGE_corr < 130)

APC_data_2016 <- as.data.frame(APC_data_2016)

####################################################################
#Bring together the separate 2016 and 2017 data files.

APC_final_2017 <- bind_rows(APC_data_2016,APC_data_2017)

#Flag total speldur across all records in spell.
APC_final_2017 <- APC_final_2017 %>% 
  group_by(ENCRYPTED_HESID, PROVSPNOPS) %>% 
  mutate(SPELDUR_2 = max(SPELDUR, na.rm=T)) %>%
  ungroup()

#Some spells have NA speldur. In these instances, calculate the duration from admission to discharge.
APC_final_2017 <- APC_final_2017 %>% 
  mutate(SPELDUR_2=ifelse(SPELDUR_2 < 0,DISDATE_2 - ADMIDATE,SPELDUR_2))

#Identify duplicate records and remove.
APC_final_2017 <- APC_final_2017 %>% 
  group_by_at(vars(ENCRYPTED_HESID, TRETSPEF, EPISTART, EPIEND, ADMIDATE, SPELDUR_2)) %>%
  mutate(Flag = ifelse(n() > 1,
                       1,
                       0))

tabyl(as.data.frame(APC_final_2017)$Flag)

#Remove spells which began more than 5 years ago and over 5 years long.
APC_final_2017 <- APC_final_2017 %>% 
  filter(ADMIDATE >= as.Date("2012-01-01") & SPELDUR_2 < 1826)

#Create a unique, and numeric, spell id for each stay. Some provspnops are identical for different individuals.
APC_final_2017 <- APC_final_2017 %>% 
  filter(Flag==0) %>% 
  group_by(ENCRYPTED_HESID,PROVSPNOPS) %>% 
  mutate(spell_id=cur_group_id()) %>%
  ungroup()

max(APC_final_2017$spell_id)

write_parquet(APC_final_2017,
              'APC_final_2017.parquet')


####################################################################
#Create a 2016 data file.


year <- '2016'

open_dataset(file.path(APC_location, year)) %>% ## grouper asks for PROVSPNO, we only have PROVSPNOPS?
  select(ENCRYPTED_HESID,
         PROVSPNOPS,
         ADMIDATE,
         EPISTART,
         EPIEND,
         DISDATE,
         EPISTAT,
         CLASSPAT,
         SPELDUR,
         ETHNOS,
         STARTAGE,
         SEX,
         LSOA11,
         PROCODE,
         PROCODET,
         SUSRECID,
         EPIORDER,
         STARTAGE,
         SEX,
         CLASSPAT,
         ADMISORC,
         ADMIMETH,
         DISDEST,
         DISMETH,
         EPIDUR,
         MAINSPEF,
         NEOCARE,
         TRETSPEF,
         #    CRITICALCAREDAYS,
         #    REHABILITATIONDAYS,
         #    SPCDAYS,
         starts_with('DIAG_'),
         starts_with('OPERTN_')
  ) %>%
  mutate(DISDATE=as.Date(DISDATE, "%Y-%m-%d"), EPISTART=as.Date(EPISTART, "%Y-%m-%d"),
         ADMIDATE=as.Date(ADMIDATE, "%Y-%m-%d"),EPIEND=as.Date(EPIEND, "%Y-%m-%d"),
         ADMICAT = as.numeric(str_sub(ADMIMETH,1,1))) %>% 
  filter(EPISTAT == 3 & !is.na(ADMIDATE) & CLASSPAT==1 & ADMICAT==2) %>% 
  collect() -> APC_data_2016


#Select out spells which take place within the time frame
APC_data_2016 <- APC_data_2016 %>% 
  group_by(ENCRYPTED_HESID, PROVSPNOPS) %>% 
  mutate(DISDATE_2 = max(DISDATE, na.rm=T)) %>% 
  filter(DISDATE_2 < as.Date("2017-01-01") & DISDATE_2 >= as.Date("2016-04-01")) %>% 
  ungroup()


#Define function to calculate mode
find_mode <- function(x) {
  u <- unique(na.omit(x))
  u[which.max(tabulate(match(x, u)))]
}
#Create variable with mode of LSOA within an admission
APC_data_2016 <- APC_data_2016 %>%
  group_by(ENCRYPTED_HESID, ADMIDATE) %>%
  mutate(LSOA_corr = find_mode(LSOA11)) %>%
  select(-LSOA11) %>% 
  ungroup()

#Create variable with mode of start age.
APC_data_2016 <- APC_data_2016 %>%
  group_by(ENCRYPTED_HESID, ADMIDATE) %>%
  mutate(STARTAGE_corr = find_mode(STARTAGE)) %>%
  select(-STARTAGE)%>%
  ungroup()

#Select out adults only.
APC_data_2016 <- APC_data_2016 %>% 
  filter(STARTAGE_corr >= 18 & STARTAGE_corr < 130)

APC_data_2016 <- as.data.frame(APC_data_2016)

####################################################################
#Create a 2015 data file.


year <- '2015'

open_dataset(file.path(APC_location, year)) %>% ## grouper asks for PROVSPNO, we only have PROVSPNOPS?
  select(ENCRYPTED_HESID,
         PROVSPNOPS,
         ADMIDATE,
         EPISTART,
         EPIEND,
         DISDATE,
         EPISTAT,
         CLASSPAT,
         SPELDUR,
         ETHNOS,
         STARTAGE,
         SEX,
         LSOA11,
         PROCODE,
         PROCODET,
         SUSRECID,
         EPIORDER,
         STARTAGE,
         SEX,
         CLASSPAT,
         ADMISORC,
         ADMIMETH,
         DISDEST,
         DISMETH,
         EPIDUR,
         MAINSPEF,
         NEOCARE,
         TRETSPEF,
         #    CRITICALCAREDAYS,
         #    REHABILITATIONDAYS,
         #    SPCDAYS,
         starts_with('DIAG_'),
         starts_with('OPERTN_')
  ) %>%
  mutate(DISDATE=as.Date(DISDATE, "%Y-%m-%d"), EPISTART=as.Date(EPISTART, "%Y-%m-%d"),
         ADMIDATE=as.Date(ADMIDATE, "%Y-%m-%d"),EPIEND=as.Date(EPIEND, "%Y-%m-%d"),
         ADMICAT = as.numeric(str_sub(ADMIMETH,1,1))) %>% 
  filter(EPISTAT == 3 & !is.na(ADMIDATE) & CLASSPAT==1 & ADMICAT==2) %>% 
  collect() -> APC_data_2015


#Select out spells which take place within the time frame
APC_data_2015 <- APC_data_2015 %>% 
  group_by(ENCRYPTED_HESID, PROVSPNOPS) %>% 
  mutate(DISDATE_2 = max(DISDATE, na.rm=T)) %>% 
  filter(DISDATE_2 < as.Date("2016-04-01") & DISDATE_2 >= as.Date("2016-01-01")) %>% 
  ungroup()


#Define function to calculate mode
find_mode <- function(x) {
  u <- unique(na.omit(x))
  u[which.max(tabulate(match(x, u)))]
}
#Create variable with mode of LSOA within an admission
APC_data_2015 <- APC_data_2015 %>%
  group_by(ENCRYPTED_HESID, ADMIDATE) %>%
  mutate(LSOA_corr = find_mode(LSOA11)) %>%
  select(-LSOA11) %>% 
  ungroup()

#Create variable with mode of start age.
APC_data_2015 <- APC_data_2015 %>%
  group_by(ENCRYPTED_HESID, ADMIDATE) %>%
  mutate(STARTAGE_corr = find_mode(STARTAGE)) %>%
  select(-STARTAGE)%>%
  ungroup()

#Select out adults only.
APC_data_2015 <- APC_data_2015 %>% 
  filter(STARTAGE_corr >= 18 & STARTAGE_corr < 130)

APC_data_2015 <- as.data.frame(APC_data_2015)

####################################################################
#Bring together the separate 2015 and 2016 data files.

APC_final_2016 <- bind_rows(APC_data_2015,APC_data_2016)

#Flag total speldur across all records in spell.
APC_final_2016 <- APC_final_2016 %>% 
  group_by(ENCRYPTED_HESID, PROVSPNOPS) %>% 
  mutate(SPELDUR_2 = max(SPELDUR, na.rm=T)) %>%
  ungroup()

#Some spells have null speldur. In these instances, calculate the duration from admission to discharge.
APC_final_2016 <- APC_final_2016 %>% 
  mutate(SPELDUR_2=ifelse(SPELDUR_2 < 0,DISDATE_2 - ADMIDATE,SPELDUR_2))

#Identify duplicate records and remove.
APC_final_2016 <- APC_final_2016 %>% 
  group_by_at(vars(ENCRYPTED_HESID, TRETSPEF, EPISTART, EPIEND, ADMIDATE, SPELDUR_2)) %>%
  mutate(Flag = ifelse(n() > 1,
                       1,
                       0))

tabyl(as.data.frame(APC_final_2016)$Flag)

#Remove spells which began more than 5 years ago and over 5 years long.
APC_final_2016 <- APC_final_2016 %>% 
  filter(ADMIDATE >= as.Date("2011-01-01") & SPELDUR_2 < 1826)

#Create a unique, and numeric, spell id for each stay. Some provspnops are identical for different individuals.
APC_final_2016 <- APC_final_2016 %>% 
  filter(Flag==0) %>% 
  group_by(ENCRYPTED_HESID,PROVSPNOPS) %>% 
  mutate(spell_id=cur_group_id()) %>%
  ungroup()

max(APC_final_2016$spell_id)

write_parquet(APC_final_2016,
              'APC_final_2016.parquet')


####################################################################
#Create a 2015 data file.


year <- '2015'

open_dataset(file.path(APC_location, year)) %>% ## grouper asks for PROVSPNO, we only have PROVSPNOPS?
  select(ENCRYPTED_HESID,
         PROVSPNOPS,
         ADMIDATE,
         EPISTART,
         EPIEND,
         DISDATE,
         EPISTAT,
         CLASSPAT,
         SPELDUR,
         ETHNOS,
         STARTAGE,
         SEX,
         LSOA11,
         PROCODE,
         PROCODET,
         SUSRECID,
         EPIORDER,
         ADMISORC,
         ADMIMETH,
         DISDEST,
         DISMETH,
         EPIDUR,
         MAINSPEF,
         NEOCARE,
         TRETSPEF,
         #    CRITICALCAREDAYS,
         #    REHABILITATIONDAYS,
         #    SPCDAYS,
         starts_with('DIAG_'),
         starts_with('OPERTN_')
  ) %>%
  mutate(DISDATE=as.Date(DISDATE, "%Y-%m-%d"), EPISTART=as.Date(EPISTART, "%Y-%m-%d"),
         ADMIDATE=as.Date(ADMIDATE, "%Y-%m-%d"),EPIEND=as.Date(EPIEND, "%Y-%m-%d"),
         ADMICAT = as.numeric(str_sub(ADMIMETH,1,1))) %>% #Create a variable which identifies if elective or non-elective.
  filter(EPISTAT == 3 & !is.na(ADMIDATE) & CLASSPAT==1 & ADMICAT==2) %>% 
  collect() -> APC_data_2015


#Select out spells which take place within the time frame
APC_data_2015 <- APC_data_2015 %>% 
  group_by(ENCRYPTED_HESID, PROVSPNOPS) %>% 
  mutate(DISDATE_2 = max(DISDATE, na.rm=T)) %>% 
  filter(DISDATE_2 < as.Date("2016-01-01") & DISDATE_2 >= as.Date("2015-04-01")) %>% 
  ungroup()


#Define function to calculate mode
find_mode <- function(x) {
  u <- unique(na.omit(x))
  u[which.max(tabulate(match(x, u)))]
}
#Create variable with mode of LSOA within an admission
APC_data_2015 <- APC_data_2015 %>%
  group_by(ENCRYPTED_HESID, ADMIDATE) %>%
  mutate(LSOA_corr = find_mode(LSOA11)) %>%
  select(-LSOA11) %>% 
  ungroup()

#Create variable with mode of start age.
APC_data_2015 <- APC_data_2015 %>%
  group_by(ENCRYPTED_HESID, ADMIDATE) %>%
  mutate(STARTAGE_corr = find_mode(STARTAGE)) %>%
  select(-STARTAGE)%>%
  ungroup()

#Select out adults only.
APC_data_2015 <- APC_data_2015 %>% 
  filter(STARTAGE_corr >= 18 & STARTAGE_corr < 130)

APC_data_2015 <- as.data.frame(APC_data_2015)


####################################################################
#Create a 2014 data file.

year <- '2014'

open_dataset(file.path(APC_location, year)) %>% ## grouper asks for PROVSPNO, we only have PROVSPNOPS?
  select(ENCRYPTED_HESID,
         PROVSPNOPS,
         ADMIDATE,
         EPISTART,
         EPIEND,
         DISDATE,
         EPISTAT,
         CLASSPAT,
         SPELDUR,
         ETHNOS,
         STARTAGE,
         SEX,
         LSOA11,
         PROCODE,
         PROCODET,
         SUSRECID,
         EPIORDER,
         ADMISORC,
         ADMIMETH,
         DISDEST,
         DISMETH,
         EPIDUR,
         MAINSPEF,
         NEOCARE,
         TRETSPEF,
         #    CRITICALCAREDAYS,
         #    REHABILITATIONDAYS,
         #    SPCDAYS,
         starts_with('DIAG_'),
         starts_with('OPERTN_')
  ) %>%
  mutate(DISDATE=as.Date(DISDATE, "%Y-%m-%d"), EPISTART=as.Date(EPISTART, "%Y-%m-%d"),
         ADMIDATE=as.Date(ADMIDATE, "%Y-%m-%d"),EPIEND=as.Date(EPIEND, "%Y-%m-%d"),
         ADMICAT = as.numeric(str_sub(ADMIMETH,1,1))) %>% 
  filter(EPISTAT == 3 & !is.na(ADMIDATE) & CLASSPAT==1 & ADMICAT==2) %>% 
  collect() -> APC_data_2014


#Select out spells which take place within the time frame
APC_data_2014 <- APC_data_2014 %>% 
  group_by(ENCRYPTED_HESID, PROVSPNOPS) %>% 
  mutate(DISDATE_2 = max(DISDATE, na.rm=T)) %>% 
  filter(DISDATE_2 < as.Date("2015-04-01") & DISDATE_2 >= as.Date("2015-01-01")) %>% 
  ungroup()


#Define function to calculate mode
find_mode <- function(x) {
  u <- unique(na.omit(x))
  u[which.max(tabulate(match(x, u)))]
}
#Create variable with mode of LSOA within an admission
APC_data_2014 <- APC_data_2014 %>%
  group_by(ENCRYPTED_HESID, ADMIDATE) %>%
  mutate(LSOA_corr = find_mode(LSOA11)) %>%
  select(-LSOA11) %>% 
  ungroup()

#Create variable with mode of start age.
APC_data_2014 <- APC_data_2014 %>%
  group_by(ENCRYPTED_HESID, ADMIDATE) %>%
  mutate(STARTAGE_corr = find_mode(STARTAGE)) %>%
  select(-STARTAGE)%>%
  ungroup()

#Select out adults only.
APC_data_2014 <- APC_data_2014 %>% 
  filter(STARTAGE_corr >= 18 & STARTAGE_corr < 130)

APC_data_2014 <- as.data.frame(APC_data_2014)

####################################################################
#Bring together the separate 2014 and 2015 data files.

APC_final_2015 <- bind_rows(APC_data_2014,APC_data_2015)

#Flag total speldur across all records in spell.
APC_final_2015 <- APC_final_2015 %>% 
  group_by(ENCRYPTED_HESID, PROVSPNOPS) %>% 
  mutate(SPELDUR_2 = max(SPELDUR, na.rm=T)) %>%
  ungroup()

#Some spells have NA speldur. In these instances, calculate the duration from admission to discharge.
APC_final_2015 <- APC_final_2015 %>% 
  mutate(SPELDUR_2=ifelse(SPELDUR_2 < 0,DISDATE_2 - ADMIDATE,SPELDUR_2))

#Identify duplicate records and remove.
APC_final_2015 <- APC_final_2015 %>% 
  group_by_at(vars(ENCRYPTED_HESID, TRETSPEF, EPISTART, EPIEND, ADMIDATE, SPELDUR_2)) %>%
  mutate(Flag = ifelse(n() > 1,
                       1,
                       0))

tabyl(as.data.frame(APC_final_2015)$Flag)

#Remove spells which began more than 5 years ago and over 5 years long.
APC_final_2015 <- APC_final_2015 %>% 
  filter(ADMIDATE >= as.Date("2010-01-01") & SPELDUR_2 < 1826)

#Create a unique, and numeric, spell id for each stay. Some provspnops are identical for different individuals.
APC_final_2015 <- APC_final_2015 %>% 
  filter(Flag==0) %>% 
  group_by(ENCRYPTED_HESID,PROVSPNOPS) %>% 
  mutate(spell_id=cur_group_id()) %>%
  ungroup()

max(APC_final_2015$spell_id)

write_parquet(APC_final_2015,
              'APC_final_2015.parquet')


####################################################################
#Append IMD data to each dataset.


#Read in the IMD data file.
LSOA_to_NHSregion <- s3read_using(read_rds,
                                  object = 'LSOA_NHSregion_lookup.RDS') %>% 
  mutate(IMDquin = ntile(IMD19, 5))

#Match the IMD data to each HES data file.
APC_final_2022 <- left_join(APC_final_2022, LSOA_to_NHSregion, by = c('LSOA_corr' = 'LSOA11CD'))
APC_final_2019 <- left_join(APC_final_2019, LSOA_to_NHSregion, by = c('LSOA_corr' = 'LSOA11CD'))
APC_final_2018 <- left_join(APC_final_2018, LSOA_to_NHSregion, by = c('LSOA_corr' = 'LSOA11CD'))
APC_final_2017 <- left_join(APC_final_2017, LSOA_to_NHSregion, by = c('LSOA_corr' = 'LSOA11CD'))
APC_final_2016 <- left_join(APC_final_2016, LSOA_to_NHSregion, by = c('LSOA_corr' = 'LSOA11CD'))
APC_final_2015 <- left_join(APC_final_2015, LSOA_to_NHSregion, by = c('LSOA_corr' = 'LSOA11CD'))

write_parquet(APC_final_2022,
              'APC_final_2022.parquet')
write_parquet(APC_final_2019,
              'APC_final_2019.parquet')
write_parquet(APC_final_2018,
              'APC_final_2018.parquet')
write_parquet(APC_final_2017,
              'APC_final_2017.parquet')
write_parquet(APC_final_2016,
              'APC_final_2016.parquet')
write_parquet(APC_final_2015,
              'APC_final_2015.parquet')
