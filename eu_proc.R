# apache spark and R interface
library(sparklyr)
# move, filter etc  data from and to database,dataframe .
library(dplyr)
# for SQL requests
library(DBI)
library(stringr)

conf <- spark_config()
conf$spark.executor.memory <- "4GB"
conf$spark.memory.fraction <- 0.2
conf$spark.executor.cores <- 2
conf$spark.dynamicAllocation.enabled <- "false"

c <- spark_connect(master= "local" , 
                   version = "2.3.0",
                   config = conf,
                   spark_home = "/home/adam/spark/spark-2.3.0-bin-hadoop2.7")


# copy imported dataset from R environment to Spark , check Connections tab in RStudio IDE
table1 <- spark_read_csv(c, name = "database", path = "/home/adam/Letöltések/EXCEL_filteringOK.csv", header = TRUE, delimiter = ";")
mellkas <- as.data.frame(table1)


# remove non-alphanumeric characters from dataset and change these to 'NA'
# spss hiba kivétele 

#dplyr like req


nums<-c("ageatsurgery", "height", "weight",
        
        "bmi","fev1litres", "fev1percent", "fevclitres",
        
        "ppofev1percent","fvcpercent","fev1ldivfvcl","dlcopercent",
        
        "ppodlcoperc","vo2max", "atelectaticsegmentatop",
        
        "prevsurgerysegmentsremoved", "functioningsegmentsresected",
        
        "diameteroflesioncm", "atelectaticsegmentsresected", 
        
        "atelectaticsegmentsrestoredbypro")



dates<-c("dob","date", "dateofdeath", "dateofdischarge")









#looky here        

# lapply(mellkas[,nums], summary)

# lapply(mellkas[,nums], function(x){sort(unique(x))})

# summary(mellkas$dateofdeath)

# options(max.print = 99999)

# lapply(mellkas[,dates],function(x) {sort(unique(as.Date.factor(x)))})



mellkas[mellkas$dateofdeath=="", ]$dateofdeath<-NA

mellkas[mellkas$date=="", ]$date<-NA

mellkas[mellkas$dateofdischarge=="", ]$dateofdischarge<-NA

mellkas[,dates]<-lapply(mellkas[,dates],as.Date.factor)



#age

mellkas[mellkas$id==128646, ]$date<-"2017-09-11"

mellkas[mellkas$date<=mellkas$dob & mellkas$date<"2004-01-01" &  !is.na(mellkas$date),"date"]<-NA

mellkas[mellkas$date==mellkas$dob & mellkas$date>"2004-01-01"&!is.na(mellkas$dob) &!is.na(mellkas$date),c("dob", "date")]<-NA

mellkas[mellkas$date>mellkas$dateofdischarge & !is.na(mellkas$date)& !is.na(mellkas$dateofdischarge), "dateofdischarge"]<-NA

mellkas[mellkas$date>mellkas$dateofdeath & !is.na(mellkas$date)& !is.na(mellkas$dateofdeath), "dateofdeath"]<-NA

mellkas<-mutate(mellkas, goodageatsurgery=abs(dob-date)/365)



#bmi

mellkas<-mutate(mellkas, height=ifelse(height>100, height/100, height) )

badweight<-mellkas$weight<=20 & mellkas$dob<"2004-01-01" | mellkas$weight>200 |is.na(mellkas$weight) 

badheight<-(is.na(mellkas$height)| (mellkas$height<1.5 & mellkas$dob<"2004-01-01") |mellkas$height>2.1)

mellkas[badweight|badheight  , c("bmi", "weight", "height")]<-NA







adat_useful <- mellkas #  subset(workdf, select = -c(41:319))

# csak a tüdőtumoros adatok  : az első kettő a doksi alapján (Zalán megerősítette)

tumordata <- subset(adat_useful, diagnosis <= 1)

# centrumok szerinti rendezés

tumordata[order(tumordata$domain), ]

# egyedi valuek az oszlopban

unique(tumordata$domain)

# hány darab van az egyes helyekből

table(tumordata$domain)



summary(tumordata)

#sapply(tumordata$diagnosis, sum, na.rm=TRUE) 

tumordata <- subset(tumordata, domain != "Hu09dl")





empty <- sapply(tumordata, function (i) all(is.na(i)))

mellkas <- tumordata[!empty]



# háníy darab hiányző value van változónként

sapply(mellkas, function(x) sum(is.na(x)))



mellkas[sapply(mellkas, is.numeric)] <- lapply(mellkas[sapply(mellkas, is.numeric)], as.factor)



library(zoo)

mellkas$dob <-as.factor(as.yearmon(mellkas$dob))

mellkas$date <- as.factor(as.yearmon(mellkas$date))

mellkas$dateofdischarge <- as.factor(as.yearmon(mellkas$dateofdischarge))

mellkas$dateofdeath <- as.factor(as.yearmon(mellkas$dateofdeath))

mellkas$goodageatsurgery <- as.factor(as.yearmon(mellkas$goodageatsurgery))





# remove unused variables 


# recode categorical variables from conitnous ones

library(plyr)

# as sex

mellkas$scode <- revalue(mellkas$sex, c("0"="Male", "1"="Female"))









mellkas$ageatsurgery <- cut(as.numeric(mellkas$ageatsurgery),
                                   
                                   breaks=c(-Inf, 20, 25, 45,60, Inf),
                                   
                                   labels=c("alacsony","child","young adult","adult","elder"))



mellkas$weight <- cut(as.numeric(mellkas$weight),
                             
                             breaks=c(-Inf, 20, 40, 60,80,100, Inf),
                             
                             labels=c("weight<20","weight20-40","weight40-60","weight60-80","weight80-100","weight>100"))



mellkas$height <- cut(as.numeric(mellkas$height),
                             
                             breaks=c(-Inf, 1, 1.2, 1.4,1.6,1.8,2.0, Inf),
                             
                             labels=c("height<100","height1-1.2","height1.2-1.4","height1.4-1.6","height1.6-1.8","height1.8-2.0","height2.0"))



mellkas$bmi <- cut(as.numeric(mellkas$bmi),
                          
                          breaks=c(-Inf, 16, 16.99, 18.49,24.99,29.99,34.99,39.99, Inf),
                          
                          labels=c("bmi<16","bmi16-16.99","bmi17-18.49","bmi18.5-24.99","bmi25-29.99","bmi30-34.99","bmi35-39.99"))





mellkas$fev1litres <- cut(as.numeric(mellkas$fev1litres),
                                 
                                 breaks=c(-Inf, 115, 161, 203,Inf),
                                 
                                 labels=c("fev1litres<115","fev1litres115-161","fev1litres161-203", "fev1litres203-385"))



mellkas$fev1percent <- cut(as.numeric(mellkas$fev1percent),
                                  
                                  breaks=c(-Inf, 159, 261, 366,Inf),
                                  
                                  labels=c("fev1percent<159","fev1percent159-261","fev1percent261-366", "fev1percent366-494"))





mellkas$fevclitres <- cut(as.numeric(mellkas$fevclitres),
                                 
                                 breaks=c(-Inf, 136, 195, 252,Inf),
                                 
                                 labels=c("fevclitres<136","fev1clitres136-195","fevclitres195-252", "fevclitres252-448"))





mellkas$ppofev1percent <- cut(as.numeric(mellkas$ppofev1percent),
                                     
                                     breaks=c(-Inf, 408, 610, 831,Inf),
                                     
                                     labels=c("ppofev1percent<408","ppofev1percent408-610","ppofev1percent610-831", "ppofev1percent831-1121"))





mellkas$fvcpercent <- cut(as.numeric(mellkas$fvcpercent),
                                 
                                 breaks=c(-Inf, 186, 267, 347,Inf),
                                 
                                 labels=c("fvcpercent<186","fvcpercent186-267","fvcpercent267-347", "fvcpercent347-472"))





mellkas$dlcopercent <- cut(as.numeric(mellkas$dlcopercent),
                                  
                                  breaks=c(-Inf, 70, 89, 111,Inf),
                                  
                                  labels=c("dlcopercent<70","dlcopercent70--89","dlcopercent89-111", "dlcopercent111-159"))



mellkas$ppodlcoperc <- cut(as.numeric(mellkas$ppodlcoperc),
                                  
                                  breaks=c(-Inf, 132, 220, 314,Inf),
                                  
                                  labels=c("ppodlcoperc<132","ppodlcoperc132-220","ppodlcoperc220-314", "ppodlcoperc314-426"))



mellkas$diameteroflesioncm<- cut(as.numeric(mellkas$diameteroflesioncm),
                                        
                                        breaks=c(-Inf, 8, 16, 25,Inf),
                                        
                                        labels=c("diameteroflesioncm<8","diameteroflesioncm8-16","diameteroflesioncm16-25", "diameteroflesioncm25-38"))



mellkas$id <- NULL

mellkas$recordnumber <- NULL

mellkas$otherdiagnosis <- NULL

mellkas$groupotherprocedure <- NULL

mellkas$entryid <- NULL

mellkas$othersurgeon <- NULL

mellkas$dob <- NULL

mellkas$scode <- NULL

mellkas$goodageatsurgery <- NULL

mellkas$notes <- NULL

mellkas$otherfreetext <- NULL

mellkas$dateofdischarge <- NULL

mellkas$date <- NULL


mellkas$atelectaticsegmentsrestoredbypro <- NULL
mellkas$groupdefinition <- NULL
mellkas$lungsubgroup <- NULL
mellkas$qualifierforrepairoflung <- NULL
mellkas$tracheaprocedure  <- NULL 
mellkas$chestwallincisionprocedures <- NULL
mellkas$ribprocedures <- NULL
mellkas$qualifierforribresection <- NULL
mellkas$mediastinumsubgroup <- NULL
mellkas$mediastinoscopyprocedures <- NULL
mellkas$mediastinumsubgroupprocedures <- NULL
mellkas$pericardialwindowqualifier <- NULL
mellkas$ligationofthoracicductprocedure <- NULL
mellkas$uppergiotherprocedure <- NULL
mellkas$diaphragmgroupprocedures <- NULL
mellkas$reasonconversion <- NULL
mellkas$prevchestwallincqualifier <- NULL



asNumeric <- function(x) as.numeric(as.character(x))
factorsNumeric <- function(d) modifyList(d, lapply(d[, sapply(d, is.factor)],   
                                                   asNumeric))

charsNumeric <- function(d) modifyList(d, lapply(d[, sapply(d, is.character)],   
                                                 asNumeric))


n_mellkas<- suppressWarnings(factorsNumeric(mellkas))
nn_mellkas <-  suppressWarnings(charsNumeric(n_mellkas))


write.csv(mellkas, file = "EU_mellkas_cleaned.csv")

library(xgboost)
# create train -test datasets from whole database


n = nrow(nn_mellkas)
trainIndex = sample(1:n, size = round(0.7*n), replace=FALSE)
train = nn_mellkas[trainIndex ,]
test = nn_mellkas[-trainIndex ,]

#remove NA-s
train2<-train[!is.na(train$sex),]
test2 <- test[!is.na(test$sex),]
str(train2)
dim(train2)
dim(test2)


# fakors and chars to numeri


adat<- as.matrix(nn_mellkas)


bstDense <- xgboost(data = as.matrix(train2[,-1]), label = train2$sex, max.depth = 7, eta = 1, nthread = 2, nrounds = 2, objective = "binary:logistic")

dtrain <- xgb.DMatrix(data = as.matrix(train2[,-1]), label = train2$sex)
bstDMatrix <- xgboost(data = dtrain, max.depth = 4, eta = 1, nthread = 4, nrounds = 10, objective = "binary:logistic", verbose = 2)

xgb.plot.tree(feature_names = train2[,-1], model = bst)




# explore the dataset using parameters of xgboost
library(ggplot2)
library(reshape2)
library(Ecdat)

# = parameters = #
# = eta candidates = #
eta=c(0.05,0.1,0.2,0.5,1)
# = colsample_bylevel candidates = #
cs=c(1/3,2/3,1)
# = max_depth candidates = #
md=c(2,4,6,10)
# = sub_sample candidates = #
ss=c(0.25,0.5,0.75,1)
# = standard model is the second value  of each vector above = #
standard=c(2,2,3,2)


set.seed(1)
conv_eta = matrix(NA,500,length(eta))
pred_eta = matrix(NA,length(test), length(eta))
colnames(conv_eta) = colnames(pred_eta) = eta
for(i in 1:length(eta)){
  params=list(eta = eta[i], colsample_bylevel=cs[standard[2]],
              subsample = ss[standard[4]], max_depth = md[standard[3]],
              min_child_weigth = 1)
  xgb=xgboost(xtrain, label = ytrain, nrounds = 500, params = params)
  conv_eta[,i] = xgb$evaluation_log$train_rmse
  pred_eta[,i] = predict(xgb, xtest)
}
