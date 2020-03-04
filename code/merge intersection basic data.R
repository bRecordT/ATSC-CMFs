#merge segment data
rm(list=ls())
gc()
setwd("G:/*****/Final Dataset")
library(readxl)
library(dplyr)
library(sqldf)

#load data
data <- read.csv("merge intersection/intersection final list.csv", header = T, stringsAsFactors = F)
rms_seg <- read.csv("PaStateRoads2019_05.csv", header = T, stringsAsFactors = F)
rms_admin <- read.csv("RMSADMIN.csv", header = T, stringsAsFactors = F)
shoulder <- read_excel("PennDOT RMS 2015.xlsx", sheet = 1)
#merfe data for each intersection leg
rms_seg <- rms_seg[,c(1:6, 12:18, 23, 32, 38, 39, 43, 79, 80)]
rms_admin <- rms_admin[,c(1, 2, 5:8, 10, 11, 13, 15:20, 32)]
shoulder <- shoulder[,c(1:3, 6:8, 11:16)]
rms_admin$SIDE_IND <- ifelse(rms_admin$SEG_BGN%%2 == 0, 1, 2)

c1 <- c("ID", "CTY1", "SR1", "SEG1", "OFF1")
c2 <- c("ID", "CTY2", "SR2", "SEG2", "OFF2")
c3 <- c("ID", "CTY3", "SR3", "SEG3", "OFF3")
c4 <- c("ID", "CTY4", "SR4", "SEG4", "OFF4")
c5 <- c("ID", "CTY5", "SR5", "SEG5", "OFF5")
c6 <- c("ID", "CTY6", "SR6", "SEG6", "OFF6")
c7 <- c("ID", "CTY7", "SR7", "SEG7", "OFF7")
c8 <- c("ID", "CTY8", "SR8", "SEG8", "OFF8")
c9 <- c("ID", "CTY9", "SR9", "SEG9", "OFF9")
c10 <- c("ID", "CTY10", "SR10", "SEG10", "OFF10")
clist <- list(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)

for (i in 1:10) {
  assign(paste0("leg_basic", i), data.frame())
}
leg_basic <- list(leg_basic1, leg_basic2, leg_basic3, leg_basic4, leg_basic5,
                  leg_basic6, leg_basic7, leg_basic8, leg_basic9, leg_basic10)

for(i in 1:10){
  leg <- data[clist[[i]]]
  names(leg) <- c("ID", "cty", "sr", "seg", "offset")
  leg_basic[[i]] <- merge(leg, rms_seg, by.x = c("cty", "sr", "seg"), by.y = c("CTY_CODE", "ST_RT_NO", "SEG_NO"),
                          all.x = T, all.y = F)
  leg_basic[[i]] <- merge(leg_basic[[i]], rms_admin, by.x = c("cty", "sr", "NORM_ADMIN"),
                          by.y = c("CTY_CODE", "ST_RT_NO", "SEG_PT_BGN"), all.x = T, all.y = F)
  leg_basic[[i]] <- merge(leg_basic[[i]], shoulder, by.x = c("cty", "sr", "seg"), by.y = c("CNTY", "SR", "SEG"),
                          all.x = T, all.y = F)
  leg_basic[[i]] <- arrange(leg_basic[[i]], ID)
  names(leg_basic[[i]]) <- paste0(names(leg_basic[[i]]), i)
}
data_basic <- do.call(cbind, leg_basic)
data < -merge(data, data_basic, by.x = c("ID"), by.y = c("ID1"))
write.csv(data, file = "basic data .csv", row.names = F)

