#merge segment data
rm(list=ls())
gc()
setwd("G:/*****/Final Dataset")
library(readxl)
library(dplyr)
library(sqldf)

#load data
data <- read.csv("merge segment/segment final list.csv", header = T, stringsAsFactors = F)
rms_admin <- read.csv("RMSADMIN.csv", header = T, stringsAsFactors = F)
shoulder <- read_excel("PennDOT RMS 2015.xlsx", sheet = 1)
#merge admin and shoulder data
data2 <- merge(data, rms_admin, by.x = c("DIST", "CTY", "SR", "NORM_ADMIN"),
               by.y = c("DISTRICT_NO", "CTY_CODE", "ST_RT_NO", "SEG_PT_BGN"), all.x = T, all.y = F)
data3 <- merge(data2, shoulder, by.x = c("DIST", "CTY", "SR", "SEG_NO"),
               by.y = c("DIST", "CNTY", "SR", "SEG"), all.x = T, all.y = F)

#assemble full segment-curve list
curve <- read.csv("Curve Inventory curve.csv", header = T, stringsAsFactors = F)
segment <- read.csv("Curve Inventory segment.csv", header = T, stringsAsFactors = F)
names(curve)[2:11] <- c("CTY", "SR", "BEG_SEG", "BEG_OFF", "END_SEG", "END_OFF", "RADIUS", "CENTRAL_ANG",
                        "CUR_LENGTH", "PRIM_SEC")
names(segment)[2:6] <- c("CTY", "SR", "SEG", "LENGTH", "PRIM_SEC")
seg_curve <- sqldf("select S.*, C.* from segment S left join curve C on S.CTY = C.CTY AND S.SR = C.SR 
                    AND S.PRIM_SEC = C.PRIM_SEC AND S.SEG >= C.BEG_SEG AND S.SEG <= C.END_SEG 
                    order by S.CTY, S.SR, S.SEG")
seg_curve <- seg_curve[-c(7, 8, 9, 17)]
seg_curve[is.na(seg_curve)] <- 0
seg_curve <- seg_curve %>% mutate(curve_length = ifelse(BEG_SEG == END_SEG, CUR_LENGTH,
                                                        ifelse(SEG == BEG_SEG, LENGTH - BEG_OFF,
                                                               ifelse(SEG == END_SEG, END_OFF, LENGTH))),
                                  degree_of_curvature = ifelse(RADIUS == 0, 0, 18000 / pi / RADIUS))
seg_curve_final <- sqldf("select CTY, SR, SEG, LENGTH, PRIM_SEC, 
                          sum(case when RADIUS!=0 then 1 else 0 end) as num_of_curve,
                          avg(RADIUS) as avg_radius, avg(CUR_LENGTH) as avg_full_curve_length_ft,
                          avg(curve_length) as avg_seg_curve_length_ft,
                          sum(curve_length) as total_seg_curve_length_ft,
                          avg(degree_of_curvature) as avg_d,
                          sum(degree_of_curvature) as total_d
                          from seg_curve group by CTY, SR, SEG")

#merge curve data
data4 <- merge(data3, seg_curve_final, by.x = c("CTY", "SR", "SEG_NO"), by.y = c("CTY", "SR", "SEG"),
               all.x = T, all.y = F)
names(data4)[9] <- 'JURIS'

#merge traffic and crash by year
#year 2018 as example
rms_traffic <- read.csv("rms/PaTrafficCounts2018.csv", header = T, stringsAsFactors = F)
data_traf <- sqldf("select d.*, t.* from data4 d left join rms_traffic t on d.CTY = t.CTY_CODE AND d.SR = t.ST_RT_NO
                    AND d.SIDE_IND = t.SIDE_IND AND d.SEG_NO >= t.SEG_BGN AND d.SEG_NO <= t.SEG_END 
                    order by d.CTY, d.SR, d.SEG_NO, t.OFFSET_BGN")
data_traf <- date_traf %>% mutate(feature_length = ifelse(SEG_BGN == SEG_END, OFFSET_END - OFFSET_BGN,
                                                          ifelse(SEG_NO == SEG_BGN, Length - OFFSET_BGN,
                                                                 ifelse(SEG_NO == SEG_END, OFFSET_END, Length))))
data_traf <- sqldf("select * from data_traf where (CTY,SR,SEG_NO,feature_length) 
                    in (select CTY,SR,SEG_NO,max(feature_length) from data_traf group by CTY,SR,SEG_NO)
                    union
                    select * from data_traf where feature_length is null")

crash <- read.csv("rms/Statewide_2018/CRASH_2018.csv", header = T, stringsAsFactors = F)
roadway <- read.csv("rms/Statewide_2018/ROADWAY_2018.csv", header = T, stringsAsFactors = F)
roadway <- subset(roadway, RDWY_SEQ_NUM == 3)
sr_crash <- merge(crash, roadway, by = c("CRN"), all.x = T) 
sr_crash <- subset(sr_crash, ROUTE != "")
names(sr_crash)[2:3] <- c("DISTRICT","COUNTY")
sr_crash_bygroup <- sqldf("select DISTRICT, COUNTY as CTY, ROUTE, SEGMENT, count(CRN) as Total,
                           sum(case when COLLISION_TYPE==0 then 1 else 0 end) as crash_0,
                           sum(case when COLLISION_TYPE==1 then 1 else 0 end) as crash_1,
                           sum(case when COLLISION_TYPE==2 then 1 else 0 end) as crash_2,
                           sum(case when COLLISION_TYPE==3 then 1 else 0 end) as crash_3,
                           sum(case when COLLISION_TYPE==4 then 1 else 0 end) as crash_4,
                           sum(case when COLLISION_TYPE==5 then 1 else 0 end) as crash_5,
                           sum(case when COLLISION_TYPE==6 then 1 else 0 end) as crash_6,
                           sum(case when COLLISION_TYPE==7 then 1 else 0 end) as crash_7,
                           sum(case when COLLISION_TYPE==8 then 1 else 0 end) as crash_8,
                           sum(case when COLLISION_TYPE==9 then 1 else 0 end) as crash_9,
                           sum(case when MAX_SEVERITY_LEVEL==0 then 1 else 0 end) as Severity_0,
                           sum(case when MAX_SEVERITY_LEVEL==1 then 1 else 0 end) as Severity_1,
                           sum(case when MAX_SEVERITY_LEVEL==2 then 1 else 0 end) as Severity_2,
                           sum(case when MAX_SEVERITY_LEVEL==3 then 1 else 0 end) as Severity_3,
                           sum(case when MAX_SEVERITY_LEVEL==4 then 1 else 0 end) as Severity_4,
                           sum(case when MAX_SEVERITY_LEVEL==8 then 1 else 0 end) as Severity_8,
                           sum(case when MAX_SEVERITY_LEVEL==9 then 1 else 0 end) as Severity_9
                           from sr_crash group by DISTRICT, CTY, ROUTE, SEGMENT")

data18<-merge(data_traf, sr_crash_bygroup, by.x = c("DIST", "CTY", "SR", "SEG_NO"),
              by.y = c("DISTRICT", "CTY", "ROUTE", "SEGMENT"), all.x = T, all.y = F)
part1 <- data18[1:123]
part2 <- data18[124:141]
part2[is.na(part2)] <- 0
data18 <- cbind(part1, part2)
data18$Year <- 2018
write.csv(data18, file = "merge segment/segment data 2018.csv", row.names = F)

#label before and after
rm(list=ls())
gc()
data <- read.csv("final/segment full data 05 to 18 v3.csv", header = T, stringsAsFactors = F)

before <- subset(data, Year >= before_start & Year <= before_end)
before$period <- "before"
after <- subset(data, Year >= after_start & Year <= after_end)
after$period <- "after"



