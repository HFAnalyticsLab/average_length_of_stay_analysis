################
# 03_Data_for_Graphing.r
# Code to reproduce the figure outputs for the Length of Stay long chart.
# November 2023
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

###########################################################################################

#Pull the datasets into one file.
age_charlson_2015_2022_all_adm <- left_join(age_charlson_2015_2019_all_adm,age_charlson_2022_all_adm,by=c('charlson_grouping', 'age_group'))

###########################################################################################
#Figure 1 - Tracking changes in severe and moderate admissions over time.

#Compute the total number of admissions, and percentage of total admissions, by complexity score.
figure_1 <- age_charlson_2015_2022_all_adm %>% 
  group_by(charlson_grouping) %>% 
  summarise(admissions_2015 = sum(admissions_2015),
            admissions_2016 = sum(admissions_2016),
            admissions_2017 = sum(admissions_2017),
            admissions_2018 = sum(admissions_2018),
            admissions_2019 = sum(admissions_2019),
            admissions_2022 = sum(admissions_2022)) %>% 
  ungroup() %>% 
  mutate(adm_2015_perc = admissions_2015/sum(admissions_2015)*100,
         adm_2016_perc = admissions_2016/sum(admissions_2016)*100,
         adm_2017_perc = admissions_2017/sum(admissions_2017)*100,
         adm_2018_perc = admissions_2018/sum(admissions_2018)*100,
         adm_2019_perc = admissions_2019/sum(admissions_2019)*100,
         adm_2022_perc = admissions_2022/sum(admissions_2022)*100) %>% 
  select(-admissions_2015,-admissions_2016,-admissions_2017,-admissions_2018,-admissions_2019,-admissions_2022)


#Transpose the data to enable for regressing.
fig1_transpose <- transpose(figure_1)
colnames(fig1_transpose) <- fig1_transpose[1,]
fig1_transpose <- fig1_transpose[-1,]
fig1_transpose$year <- seq(from=2015, length.out=nrow(fig1_transpose), by=1) 
last_row_index <- nrow(fig1_transpose)
fig1_transpose$year[last_row_index] <- 2022

#Select out admissions classified as severe.
severe_data <- fig1_transpose %>% 
  select(year,Severe)

#Filter out 2022 so regressing on previous years only.
severe_data_filtered <- severe_data %>% 
  filter(year != 2022)

rownames(severe_data) <- NULL

#Run a linear model against the severe data.
severe_linear_model <- lm(Severe ~ year, data=severe_data_filtered)

#Create an file with years we want to predict for.
estimate_years <- data.frame(year = c(2015:2022))

#Predict proportion of admissions for each year
severe_predict <- predict(severe_linear_model,newdata=estimate_years)

#Create dataframe with predicted data and years.
severe_predictions <- data.frame(year=estimate_years$year, severe_trend=severe_predict)

#Bring together estimated proportions and actual proportions for severe admissions.
figure_1a <- right_join(severe_data, severe_predictions, by='year') %>% 
  arrange(year)



#Select out admissions classified as moderate.
moderate_data <- fig1_transpose %>% 
  select(year,Moderate)

#Filter out 2022 so regressing on previous years only.
moderate_data_filtered <- moderate_data %>% 
  filter(year != 2022)

rownames(moderate_data) <- NULL


moderate_linear_model <- lm(Moderate ~ year, data=moderate_data_filtered)
estimate_years <- data.frame(year = c(2015:2022))
moderate_predict <- predict(moderate_linear_model,newdata=estimate_years)
moderate_predictions <- data.frame(year=estimate_years$year, moderate_trend=moderate_predict)


figure_1b <- right_join(moderate_data, moderate_predictions, by='year') %>% 
  arrange(year)


###########################################################################################
#Figure 2 - Comparing LOS for non-Covid 2022 vs previous years.


#Filter for non-covid stays only. 
acc_2022 <- age_charlson_covid_2022 %>% 
  filter(covid_flag==0) %>% 
  select(-covid_flag) %>% 
  mutate(los_2022 = beddays_2022/admissions_2022)

#Join previous years with 2022 non-covid data.
age_charlson_2015_2022 <- left_join(age_charlson_2015_2019,acc_2022,by=c('charlson_grouping', 'age_group'))

#Calculate the non-standardised LOS for each year.
non_standardised_los <- age_charlson_2015_2022 %>% 
  mutate(standardised_flag=0) %>% 
  group_by(standardised_flag) %>% 
  summarise(admissions_2015=sum(admissions_2015),
            beddays_2015=sum(beddays_2015),
            admissions_2016=sum(admissions_2016),
            beddays_2016=sum(beddays_2016),
            admissions_2017=sum(admissions_2017),
            beddays_2017=sum(beddays_2017),
            admissions_2018=sum(admissions_2018),
            beddays_2018=sum(beddays_2018),
            admissions_2019=sum(admissions_2019),
            beddays_2019=sum(beddays_2019),
            admissions_2022=sum(admissions_2022),
            beddays_2022=sum(beddays_2022)) %>% 
  mutate(los_2015 = beddays_2015/admissions_2015,
         los_2016 = beddays_2016/admissions_2016,
         los_2017 = beddays_2017/admissions_2017,
         los_2018 = beddays_2018/admissions_2018,
         los_2019 = beddays_2019/admissions_2019,
         los_2022 = beddays_2022/admissions_2022) %>% 
  select(standardised_flag,los_2015,los_2016,los_2017,los_2018,los_2019,los_2022) %>% 
  ungroup()


#Create a variable with 2015 age-complexity distribution and use this for standardisation across the other years.
standardised_los <- age_charlson_2015_2022 %>% 
  mutate(distribution_2015 = admissions_2015/sum(admissions_2015)) %>% 
  mutate(standardised_2015 = (beddays_2015/admissions_2015)*distribution_2015,
         standardised_2016 = (beddays_2016/admissions_2016)*distribution_2015,
         standardised_2017 = (beddays_2017/admissions_2017)*distribution_2015,
         standardised_2018 = (beddays_2018/admissions_2018)*distribution_2015,
         standardised_2019 = (beddays_2019/admissions_2019)*distribution_2015,
         standardised_2022 = (beddays_2022/admissions_2022)*distribution_2015) %>% 
  mutate(standardised_flag=1) %>% 
  group_by(standardised_flag) %>% 
  summarise(los_2015=sum(standardised_2015),
            los_2016=sum(standardised_2016),
            los_2017=sum(standardised_2017),
            los_2018=sum(standardised_2018),
            los_2019=sum(standardised_2019),
            los_2022=sum(standardised_2022)) %>% 
  ungroup()

#Bring together the non_standardised and standardised LOS.
figure_2 <- rbind(non_standardised_los,standardised_los)

figure_2 <- figure_2 %>% 
  mutate(los_2016_diff = los_2016 - los_2015,
         los_2017_diff = los_2017 - los_2016,
         los_2018_diff = los_2018 - los_2017,
         los_2019_diff = los_2019 - los_2018,
         los_2022_diff = los_2022 - los_2019,
         
         los_2016_perc_diff = los_2016_diff/los_2015*100,
         los_2017_perc_diff = los_2017_diff/los_2016*100,
         los_2018_perc_diff = los_2018_diff/los_2017*100,
         los_2019_perc_diff = los_2019_diff/los_2018*100,
         los_2022_perc_diff = los_2022_diff/los_2019*100) %>% 
  select(-los_2015,-los_2016,-los_2017,-los_2018,-los_2019,-los_2022)



###########################################################################################
#Figure 3 - Comparing the different LOS for each type of Covid grouping.


#Create a new flag that combines all covid.
total_covid_2022 <- age_charlson_covid_2022 %>% 
  filter(covid_flag %in% c(1,2)) %>% 
  group_by(charlson_grouping, age_group) %>% 
  summarise(covid_flag = 3, 
            admissions_2022 = sum(admissions_2022),
            beddays_2022 = sum(beddays_2022)) %>% 
  ungroup()

#Bring together the separate covid flags and the new combined covid flag.
covid_2022 <- bind_rows(age_charlson_covid_2022,total_covid_2022) %>% 
  mutate(los_2022=beddays_2022/admissions_2022)

#Calculate the age-complexity distribution of the non-covid group.
distribution_2022 <- covid_2022 %>% 
  filter(covid_flag==0) %>% 
  mutate(non_covid_distribution = admissions_2022/sum(admissions_2022)) %>% 
  select(charlson_grouping,age_group,non_covid_distribution)

#Use the non-covid distribution to calculate the standardised covid LOS.
figure_3 <- left_join(covid_2022,distribution_2022, by = c('charlson_grouping','age_group')) %>% 
  mutate(standardised_los = los_2022*non_covid_distribution) %>% 
  group_by(covid_flag) %>% 
  summarise(los_2022 = sum(standardised_los)) %>% 
  ungroup()


###########################################################################################
#Figure 4 - Change in admissions by average length of stay in 2022, compared with 2019.


#Calculate the total number of covid and non-covid admissions at each length of stay
days_2022 <- APC_final_2022 %>% 
  filter(SPELDUR<90) %>% 
  mutate(non_covid_adm = ifelse(covid_flag==0,1,0),
         covid_adm = ifelse(covid_flag>=1,1,0)) %>% 
  group_by(SPELDUR) %>% 
  summarise(non_covid_adm_2022=sum(non_covid_adm),
            covid_adm_2022=sum(covid_adm),
            total_adm_2022=n()) %>% 
  ungroup() %>% 
  mutate(total_beds_2022=total_adm_2022*SPELDUR)


#Calculate the total number of covid and non-covid admissions at each length of stay
days_2019 <- APC_final_2019 %>% 
  filter(SPELDUR<90) %>% 
  group_by(SPELDUR) %>% 
  summarise(total_adm_2019=n()) %>% 
  ungroup() %>% 
  mutate(total_beds_2019=total_adm_2019*SPELDUR)

#Calculate the absolute difference between 2019 and 2022 beddays. Then calculate the covid excess and non-covid excess.
figure_4a <- left_join(days_2019,days_2022, by = 'SPELDUR') %>% 
  mutate(absolute_diff = total_adm_2022-total_adm_2019,
         covid_excess = covid_adm_2022,
         non_covid_excess = absolute_diff-covid_adm_2022) %>%
  select(SPELDUR,absolute_diff,covid_excess,non_covid_excess)


#Calculate the proportional difference between 2019 and 2022 beddays. Then calculate the covid excess and non-covid excess.
figure_4b <- left_join(days_2019,days_2022, by = 'SPELDUR') %>% 
mutate(absolute_diff = total_adm_2022-total_adm_2019,
       net_change = absolute_diff/total_adm_2019,
       covid_excess = covid_adm_2022/total_adm_2019,
       non_covid_excess = (absolute_diff-covid_adm_2022)/total_adm_2019) %>% 
  select(SPELDUR,net_change, covid_excess, non_covid_excess)


###########################################################################################
#Figure 5 - Proportion of emergency admissions across length of stay.


#Calculate the percentage of non-covid to covid admissions at each length of stay (up to 90 days). 
figure_5 <- APC_final_2022 %>% 
  filter(SPELDUR<90) %>% 
  group_by(covid_flag,SPELDUR) %>% 
  summarise(no_admissions = n()) %>% 
  ungroup() %>% 
  group_by(SPELDUR) %>% 
  mutate(admission_perc = no_admissions/sum(no_admissions)) %>% 
  ungroup() %>% 
  select(-no_admissions)

