# IntraDayPrediction
The repository for the IntraDayTrading Algorithm based on Clustering. 
Development with Machine Learning as a precursor for a commercial product under **Edinture Pvt Ltd.**

Sunil Pai G | Tanush Vinay | Paaras Belandor
----------- | ------------ | ---------------

## Features
The algorithm makes use of a simple correlation of values to extrapolate the behaviour of the market for the rest of the day.
At every 3 minute interval, starting from 09:30, the algorithm recalculates the best correlated day with the live data and plots the graph/Uploads the predicted date and correlation coefficient to Redis Cache.
