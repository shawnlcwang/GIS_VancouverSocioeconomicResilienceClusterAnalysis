library (BARD)
library (maptools)
library (rJava)
library (iplots)

# shapefile path
# shapefiles <- readShapeSpatial("D:\\MSc Thesis\\CCAR Database\\SoRI\\MAUP\\2006\\MAUP_2006_DA_R.shp")
# MAUP_2006_DA <- importBardShape(file.path(system.file("shapefiles", package="BARD")))

# Plan input output. Reading and writing plans in various formats.
MAUP_2006_DA <- importBardShape("D:\\MSc Thesis\\CCAR Database\\SoRI\\MAUP\\2006\\MAUP_2006_DA_R.shp", id="GEOGRAPHY",  wantplan=FALSE) 

# Initial plan generation. Quick heuristics for generating random plans, or plans based on a fixed set of criteria
# choose number of districts
ndists <- 400
kplan <- createKmeansPlan(MAUP_2006_DA,ndists)
rplan <- createRandomPlan(MAUP_2006_DA,ndists)
# rplan2 <- createRandomPopPlan(MAUP_2006_DA,ndists,predvar="POPULATION")
plot(kplan, col = colorRampPalette(c("red", "grey"))(ndists), axes = FALSE)
plot(rplan, col = colorRampPalette(c("red", "grey"))(ndists), axes = FALSE)
# plot(rplan2, col = colorRampPalette(c("red", "grey"))(ndists), axes = FALSE)
plot(diff(kplan, rplan), plotall = TRUE, col = colorRampPalette(c("red", "grey"))(ndists), axes = FALSE, horizontal = FALSE)

# reportPlans(plans=list("kmeans"=kplan,"random plan"=rplan,"random pop"=rplan2), doplot=TRUE)
# portPlans(plans=list("kmeans"=kplan,"random"=rplan,"random pop"=rplan2), doplot=TRUE)
reportPlans(plans=list("kmeans"=kplan,"random"=rplan,"random pop"=rplan2), doplot=TRUE)

# Interactive plan editing. Adjust plans interactively, using a mouse.
# if (require("iplots",quietly=TRUE)) {
#   rplan<-editPlanInteractive(rplan,calcPopScore,predvar="POPULATION")
# }

# district sampling - quick
randomDists<-quickSampleDistricts(10,MAUP_2006_DA,ndists)
distscores<- scorePlans(randomDists,scoreFUNs=list("LWCompact"=calcLWCompactScore,"PACompact"=calcPACompactScore,"PopScore"=calcPopScore ))
plot(distscores[2:3])


# Plan scoring. Scoring functions for use in plan refinement, profiling, and comparison
calcContiguityScore(rplan)
calcLWCompactScore(rplan)
calcReockScore(plan)
calcPopScore(rplan)
calcRangeScore(rplan, targrange=c(.01,.99))
calcGroupScore(rplan,groups=list(c(1:10),c(100:120)),penalties=c(1,2))
kplan2<-kplan1<-kplan
cl<-cbind(c(318,320),c(kplan1[318],kplan1[320]))
calcPopScore(kplan2,lastscore=calcPopScore(kplan1),changelist=cl) == calcPopScore(kplan2)
calcHolesScore(plan)


myScore<-function(plan,...)  {
  return(calcContiguityScore(plan,...))
  
}      

# Plan refinement. Multi-criteria optimization heuristics for refining plans to meet specified goals.
# This works better, but will take a while
improvedRplan<-refineAnnealPlan(plan=rplan2, score.fun=myScore, historysize=0, dynamicscoring=FALSE, tracelevel=1)


# Plan profiling and exploration. Generate profiles of plans to explore tradeoffs among redistricting criteria. This can be used in conjunction with snow to distribute plan generation across a computing cluster

samples<-samplePlans(kplan, score.fun=myScore, ngenplans=10, gen.fun = "createRandomPlan", refine.fun="refineNelderPlan",refine.args=list(maxit=200,dynamicscoring=TRUE))

profplans<-profilePlans(  list(kplan,rplan), score.fun=calcContiguityScore, addscore.fun=calcPopScore, numevals=2, weight=c(0,.5,1), refine.fun="refineNelderPlan",refine.args=list(maxit=200,dynamicscoring=TRUE) )


# reportPlans(plans=list("kmeans"=kplan,"random plan"=rplan,"random pop"=rplan2), doplot=TRUE)
summary(samples)
plot(summary(samples))
reportPlans(samples)
plot(summary(profplans))