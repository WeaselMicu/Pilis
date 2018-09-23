library(xgboost)
library(ggplot2)
library(reshape2)
library(Ecdat)

set.seed(1)
N = 1000
k = 10
x = matrix(rnorm(N*k),N,k)
b = (-1)^(1:k)
yaux=(x%*%b)^2
e = rnorm(N)
y=yaux+e

# = select train and test indexes = #
train=sample(1:N,800)
test=setdiff(1:N,train)

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

# = train and test data = #
xtrain = x[train,]
ytrain = y[train]
xtest = x[test,]
ytest = y[test]


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

conv_eta = data.frame(iter=1:500, conv_eta)
conv_eta = melt(conv_eta, id.vars = "iter")
ggplot(data = conv_eta) + geom_line(aes(x = iter, y = value, color = variable))

(RMSE_eta = sqrt(colMeans((ytest-pred_eta)^2)))

set.seed(1)
conv_cs = matrix(NA,500,length(cs))
pred_cs = matrix(NA,length(test), length(cs))
colnames(conv_cs) = colnames(pred_cs) = cs
for(i in 1:length(cs)){
  params = list(eta = eta[standard[1]], colsample_bylevel = cs[i],
                subsample = ss[standard[4]], max_depth = md[standard[3]],
                min_child_weigth = 1)
  xgb=xgboost(xtrain, label = ytrain,nrounds = 500, params = params)
  conv_cs[,i] = xgb$evaluation_log$train_rmse
  pred_cs[,i] = predict(xgb, xtest)
}

conv_cs = data.frame(iter=1:500, conv_cs)
conv_cs = melt(conv_cs, id.vars = "iter")
ggplot(data = conv_cs) + geom_line(aes(x = iter, y = value, color = variable))

(RMSE_cs = sqrt(colMeans((ytest-pred_cs)^2)))

set.seed(1)
conv_md=matrix(NA,500,length(md))
pred_md=matrix(NA,length(test),length(md))
colnames(conv_md)=colnames(pred_md)=md
for(i in 1:length(md)){
  params=list(eta=eta[standard[1]],colsample_bylevel=cs[standard[2]],
              subsample=ss[standard[4]],max_depth=md[i],
              min_child_weigth=1)
  xgb=xgboost(xtrain, label = ytrain,nrounds = 500,params=params)
  conv_md[,i] = xgb$evaluation_log$train_rmse
  pred_md[,i] = predict(xgb, xtest)
}

conv_md=data.frame(iter=1:500,conv_md)
conv_md=melt(conv_md,id.vars = "iter")
ggplot(data=conv_md)+geom_line(aes(x=iter,y=value,color=variable)
                               
(RMSE_md=sqrt(colMeans((ytest-pred_md)^2)))

set.seed(1)
conv_ss=matrix(NA,500,length(ss))
pred_ss=matrix(NA,length(test),length(ss))
colnames(conv_ss)=colnames(pred_ss)=ss
for(i in 1:length(ss)){
  params=list(eta=eta[standard[1]],colsample_bylevel=cs[standard[2]],
              subsample=ss[i],max_depth=md[standard[3]],
              min_child_weigth=1)
  xgb=xgboost(xtrain, label = ytrain,nrounds = 500,params=params)
  conv_ss[,i] = xgb$evaluation_log$train_rmse
  pred_ss[,i] = predict(xgb, xtest)
}

conv_ss=data.frame(iter=1:500,conv_ss)
conv_ss=melt(conv_ss,id.vars = "iter")
ggplot(data=conv_ss)+geom_line(aes(x=iter,y=value,color=variable))

(RMSE_ss=sqrt(colMeans((ytest-pred_ss)^2)))


                               
                               