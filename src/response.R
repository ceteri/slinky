## Copyright (C) 2010, Paco Nathan. This work is licensed under
## the BSD License. To view a copy of this license, visit:
##    http://creativecommons.org/licenses/BSD/
## or send a letter to:
##    Creative Commons, 171 Second Street, Suite 300
##    San Francisco, California, 94105, USA
##
## @author Paco Nathan <ceteri@gmail.com>

## create a graph for relative frequence vs. HTTP response time

data <- read.csv("response.tsv", sep=' ', header=F)

t <- data[,2]
c <- data[,1]

sum_prod <- 0
sum_count <- sum(c)

for (i in 1:length(t)) sum_prod <- sum_prod + (t[i] * c[i])

mean <- sum_prod/sum_count
title = paste("HTTP response time distrib", "\n", "mean =", round(mean, 2))

plot(t, c / sum_count, type="l", ylab="prob. freq.", xlab="response time (sec)", main=title)
abline(v=mean, col="red")
