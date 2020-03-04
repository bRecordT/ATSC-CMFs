#####
rm(list=ls())
gc()
setwd("G:/*****/Final Dataset")
library(dplyr)
library(sqldf)
library(lubridate)

#EB Analysis
#intersection
before <- read.csv("final/analysis/main intersection before v2.csv", header = T, stringsAsFactors = F)
after <- read.csv("final/analysis/main intersection after v2.csv", header = T, stringsAsFactors = F)
data <- rbind(before, after)

attach(data)
data$N_SPF <- ifelse(leg3_urban_arterial==1|leg3_urban_collector==1,exp(-5.119)*(major_aadt^0.398)*(minor_aadt^0.214)*
                     exp(0.110*ELTMaj+0.112*ELTMin+0.152*MajPSL30_35+0.351*MajPSL40p-0.182*dist030809+0.146*dist0511),
                     ifelse(leg4_urban_arterial==1,exp(-5.307)*(major_aadt^0.362)*(minor_aadt^0.338)*
                            exp(0.051*ELTMaj+0.128*ERTMaj+0.045*ELTMin+0.041*ERTMin+
                                  0.084*MajPSL40_45+0.293*MajPSL50_55+0.073*MinPSL35p-0.247*dist010212-
                                  0.334*dist0310+0.104*dist4-0.142*dist0809-0.047*dist11),
                            ifelse(leg4_urban_collector==1,exp(-7.009)*(major_aadt^0.566)*(minor_aadt^0.297),
                                   1.05*exp(-5.307)*(major_aadt^0.362)*(minor_aadt^0.338)*
                                     exp(0.051*ELTMaj+0.128*ERTMaj+0.045*ELTMin+0.041*ERTMin+
                                           0.084*MajPSL40_45+0.293*MajPSL50_55+0.073*MinPSL35p-0.247*dist010212-
                                           0.334*dist0310+0.104*dist4-0.142*dist0809-0.047*dist11)))
                   )

data$alpha <- ifelse(leg3_urban_arterial == 1 | leg3_urban_collector == 1, 0.395,
                     ifelse(leg4_urban_arterial == 1, 0.358,
                            ifelse(leg4_urban_collector == 1, 0.196, 0.358))
                     )

detach(data)

before <- subset(data, period == "before")
after <- subset(data, period == "after")

sum_before <- sqldf("select *, sum(Total) as Obs_sum_before, sum(N_SPF) as SPF_sum_before from before group by ID")
sum_after <- sqldf("select *, sum(Total) as Obs_sum_after, sum(N_SPF) as SPF_sum_after from after group by ID")

sum_before$w <- 1/(1 + sum_before$alpha * sum_before$SPF_sum_before)
sum_before$sum_EB_before <- sum_before$w * sum_before$SPF_sum_before + (1 - sum_before$w) * sum_before$Obs_sum_before

sumdata <- merge(sum_before[,c(1:96, 136:147, 150:154)], sum_after[,c(1, 151, 152)], by = "ID")
sumdata$r <- sumdata$SPF_sum_after / sumdata$SPF_sum_before
sumdata$sum_EB_after <- sumdata$sum_EB_before * sumdata$r

#estimate CMFs for various groups
#including intersection location, intersection configuration, engineering district, algorithm type, etc.
#main intersection as example
group1 <- subset(sumdata, is_par_int == 0 & is_up_down_int == 0)
total_obs <- sum(group1$Obs_sum_after)
total_EB <- sum(group1$sum_EB_after)
var_total_EB <- sum((group1$r)^2 * (1 - group1$w) * group1$sum_EB_after)
cmf <- total_obs / (total_EB * (1 + var_total_EB/total_EB^2))
var_total_obs <- sum(group1$Obs_sum_after)
cmf_se <- sqrt(cmf^2 * ((var_total_obs / total_obs^2) + (var_total_EB / total_EB^2)) / (1 + var_total_EB / total_EB^2)^2)


