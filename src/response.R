data <- read.csv("/Users/paco/Desktop/crawl/dist.tsv", sep=' ', header=F)

t <- data[,2]
c <- data[,1]

sum_prod <- 0
sum_count <- sum(c)

for (i in 1:length(t)) sum_prod <- sum_prod + (t[i] * c[i])

mean <- sum_prod/sum_count
title = paste("HTTP response time distrib", "\n", "mean =", round(mean, 2))

plot(t, c / sum_count, type="l", ylab="prob. freq.", xlab="response time (sec)", main=title)
abline(v=mean, col="red")
