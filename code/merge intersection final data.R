#merge segment data
rm(list=ls())
gc()
setwd("G:/*****/Final Dataset")
library(readxl)
library(dplyr)
library(sqldf)

#merge aadt and crash by year
#year 2018 as example
data <- read.csv("basic with traffic info.csv", header = T, stringsAsFactors = F)
rms_traf <- read.csv("rms/PaTrafficCounts2018.csv", header = T, stringsAsFactors = F)
rms_traf <- select(rms_traf, CTY_CODE, ST_RT_NO, SEG_BGN, OFFSET_BGN, SEG_END, OFFSET_END, SIDE_IND, CUR_AADT)

crash <- read.csv("rms/Statewide_2018/CRASH_2018.csv", header = T, stringsAsFactors = F)
roadway <- read.csv("rms/Statewide_2018/ROADWAY_2018.csv", header = T, stringsAsFactors = F)
roadway <- subset(roadway, RDWY_SEQ_NUM == 3)
sr_crash <- merge(crash, roadway, by = c("CRN"), all.x = T) 
sr_crash <- subset(sr_crash, ROUTE!="")
names(sr_crash)[2:3] <- c("DISTRICT", "COUNTY")

#merge data for each leg
c1 <- c("ID", "CTY1", "SR1", "SEG1", "OFF1", "SIDE_IND1", "SEG_LNGTH_1")
c2 <- c("ID", "CTY2", "SR2", "SEG2", "OFF2", "SIDE_IND2", "SEG_LNGTH_2")
c3 <- c("ID", "CTY3", "SR3", "SEG3", "OFF3", "SIDE_IND3", "SEG_LNGTH_3")
c4 <- c("ID", "CTY4", "SR4", "SEG4", "OFF4", "SIDE_IND4", "SEG_LNGTH_4")
c5 <- c("ID", "CTY5", "SR5", "SEG5", "OFF5", "SIDE_IND5", "SEG_LNGTH_5")
c6 <- c("ID", "CTY6", "SR6", "SEG6", "OFF6", "SIDE_IND6", "SEG_LNGTH_6")
c7 <- c("ID", "CTY7", "SR7", "SEG7", "OFF7", "SIDE_IND7", "SEG_LNGTH_7")
c8 <- c("ID", "CTY8", "SR8", "SEG8", "OFF8", "SIDE_IND8", "SEG_LNGTH_8")
c9 <- c("ID", "CTY9", "SR9", "SEG9", "OFF9", "SIDE_IND9", "SEG_LNGTH_9")
c10 <- c("ID", "CTY10", "SR10", "SEG10", "OFF10", "SIDE_IND10", "SEG_LNGTH_10")
clist <- list(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)

for (i in 1:10) {
  assign(paste0("leg_crash", i), data.frame())
}
leg_crash <- list(leg_crash1, leg_crash2, leg_crash3, leg_crash4, leg_crash5,
                  leg_crash6, leg_crash7, leg_crash8, leg_crash9, leg_crash10)

for(i in 1:10){
  leg <- data[clist[[i]]]
  names(leg) <- c("ID", "cty", "sr", "seg", "offset", "SIDE_IND", "length")
  leg_traf <- sqldf("select d.*, t.* from leg d left join rms_traf t on d.cty = t.CTY_CODE AND d.sr = t.ST_RT_NO
                     AND d.SIDE_IND = t.SIDE_IND AND d.SEG >= t.SEG_BGN AND d.SEG <= t.SEG_END 
                     order by d.cty, d.sr, d.seg, t.OFFSET_BGN")
  leg_traf$feature_length <- ifelse(leg_traf$SEG_BGN == leg_traf$SEG_END, leg_traf$OFFSET_END - leg_traf$OFFSET_BGN,
                                    ifelse(leg_traf$seg == leg_traf$SEG_BGN, leg_traf$length - leg_traf$OFFSET_BGN,
                                           ifelse(leg_traf$seg == leg_traf$SEG_END, leg_traf$OFFSET_END, leg_traf$length)))
  leg_traf <- sqldf("select leg_traf.* from leg_traf where (cty, sr, seg, feature_length) 
                     in (select cty, sr, seg, max(feature_length) from leg_traf group by cty, sr, seg) 
                     union
                     select leg_traf.* from leg_traf where feature_length is null")
  leg_crash[[i]] <- sqldf("select l.*, count(c.CRN) as Total,
                          sum(case when c.COLLISION_TYPE==0 then 1 else 0 end) as crash_0,
                          sum(case when c.COLLISION_TYPE==1 then 1 else 0 end) as crash_1,
                          sum(case when c.COLLISION_TYPE==2 then 1 else 0 end) as crash_2,
                          sum(case when c.COLLISION_TYPE==3 then 1 else 0 end) as crash_3,
                          sum(case when c.COLLISION_TYPE==4 then 1 else 0 end) as crash_4,
                          sum(case when c.COLLISION_TYPE==5 then 1 else 0 end) as crash_5,
                          sum(case when c.COLLISION_TYPE==6 then 1 else 0 end) as crash_6,
                          sum(case when c.COLLISION_TYPE==7 then 1 else 0 end) as crash_7,
                          sum(case when c.COLLISION_TYPE==8 then 1 else 0 end) as crash_8,
                          sum(case when c.COLLISION_TYPE==9 then 1 else 0 end) as crash_9,
                          sum(case when c.MAX_SEVERITY_LEVEL==0 then 1 else 0 end) as Severity_0,
                          sum(case when c.MAX_SEVERITY_LEVEL==1 then 1 else 0 end) as Severity_1,
                          sum(case when c.MAX_SEVERITY_LEVEL==2 then 1 else 0 end) as Severity_2,
                          sum(case when c.MAX_SEVERITY_LEVEL==3 then 1 else 0 end) as Severity_3,
                          sum(case when c.MAX_SEVERITY_LEVEL==4 then 1 else 0 end) as Severity_4,
                          sum(case when c.MAX_SEVERITY_LEVEL==8 then 1 else 0 end) as Severity_8,
                          sum(case when c.MAX_SEVERITY_LEVEL==9 then 1 else 0 end) as Severity_9 from leg_traf l
                          left join sr_crash c on l.cty = c.COUNTY AND l.sr = c.ROUTE and l.seg = c.SEGMENT and
                          (c.OFFSET > l.offset - 251 And c.OFFSET < l.offset + 251) 
                          group by l.ID, l.cty, l.sr, l.seg, l.offset")
  
  leg_crash[[i]] <- arrange(leg_crash[[i]], ID)
  names(leg_crash[[i]]) <- paste0(names(leg_crash[[i]]), "_", i)
}

data_crash <- do.call(cbind, leg_crash)
var_remove <- c("length", "SIDE_IND", "ST_RT_NO", "CTY_CODE", "SEG_BGN", "OFFSET_BGN", "SEG_END",
                "OFFSET_END", "feature_length")
for (i in 1: length(var_remove)){
  data_crash <- select(data_crash, -starts_with(var_remove[i]))
}

#aggregate leg crash for intersection
col_name <- c("ID", "cty", "sr", "seg", "offset", "aadt", "Total", "crash_0", "crash_1", "crash_2", "crash_3",
              "crash_4", "crash_5", "crash_6", "crash_7", "crash_8", "crash_9", "severity_0", "severity_1",
              "severity_2", "severity_3", "severity_4", "severity_8", "severity_9")

data_crash_transform <- data_crash
names(data_crash_transform) <- rep(col_name, 10)
data_crash_transform <- rbind(data_crash_transform[1:24], data_crash_transform[25:48], data_crash_transform[49:72],
                              data_crash_transform[73:96], data_crash_transform[97:120], data_crash_transform[121:144],
                              data_crash_transform[145:168], data_crash_transform[169:192], data_crash_transform[193:216],
                              data_crash_transform[217:240])

agg_crash <- data_crash_transform %>% group_by(ID) %>% distinct(cty, sr, seg, offset, .keep_all = T) %>%
  summarise(Total = sum(Total), 
            crash_0 = sum(crash_0), 
            crash_1 = sum(crash_1), 
            crash_2 = sum(crash_2),
            crash_3 = sum(crash_3), 
            crash_4 = sum(crash_4), 
            crash_5 = sum(crash_5),
            crash_6 = sum(crash_6),
            crash_7 = sum(crash_7),
            crash_8 = sum(crash_8),
            crash_9 = sum(crash_9),
            severity_0 = sum(severity_0),
            severity_1 = sum(severity_1),
            severity_2 = sum(severity_2),
            severity_3 = sum(severity_3),
            severity_4 = sum(severity_4),
            severity_8 = sum(severity_8),
            severity_9 = sum(severity_9))


data_crash <- data_crash[,c(1, 6, 30, 54, 78, 102, 126, 150, 174, 198, 222)]
data_crash <- merge(data_crash, agg_crash, by.x = c("ID_1"), by.y = c("ID"))

data18 <- merge(data, data_crash, by.x = c("ID"), by.y = c("ID_1"))
data18$Year <- 2018
write.csv(data18, file = "merge intersection/intersection data 2018.csv", row.names = F)

#find unknown aadt
#aggregate aadt
rm(list=ls())
gc()
data <- read.csv("merge intersection/intersection full data 05 to 18 v2.csv", header = T, stringsAsFactors = F)

data$aadt_leg1<-ifelse(data$aadt_leg1==0,NA,data$aadt_leg1)
data$aadt_leg2<-ifelse(data$aadt_leg2==0,NA,data$aadt_leg2)
data$aadt_leg3<-ifelse(data$aadt_leg3==0,NA,data$aadt_leg3)
data$aadt_leg4<-ifelse(data$aadt_leg4==0,NA,data$aadt_leg4)
data$aadt_leg5<-ifelse(data$aadt_leg5==0,NA,data$aadt_leg5)


data <- data %>% mutate(aadt_leg1 = ifelse(aadt_leg1 == 0, NA, aadt_leg1),
                        aadt_leg2 = ifelse(aadt_leg2 == 0, NA, aadt_leg2),
                        aadt_leg3 = ifelse(aadt_leg3 == 0, NA, aadt_leg3),
                        aadt_leg4 = ifelse(aadt_leg4 == 0, NA, aadt_leg4),
                        aadt_leg5 = ifelse(aadt_leg5 == 0, NA, aadt_leg5)) %>%
  mutate(aadt_approach1 = rowMeans(.[,c("aadt_leg1", "aadt_leg2")], na.rm = T),
         aadt_approach2 = rowMeans(.[,c("aadt_leg3", "aadt_leg4")], na.rm = T)) %>%
  mutate_at(147:148, list(~round(.))) %>%
  mutate(major_aadt = ifelse(Major12 == 1, aadt_approach1, aadt_approach2),
         minor_aadt = ifelse(Major12 == 1, aadt_approach2, aadt_approach1))

#label before and after
before <- subset(data, Year >= before_start & Year <= before_end)
before$period <- "before"
after <- subset(data,Year >= after_start & Year <= after_end)
after$period <- "after"


