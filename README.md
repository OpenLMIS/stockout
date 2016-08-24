# Stockout Prediction

This is a technical spike, trying to predict stockouts based on historical stock movement data in OpenLMIS. If stockouts can be predicted with some machine learning algorithm, then the MoHs would be able to prevent them from happening.
 
## Training Data Set

The training data set is from Mozambique ESMS implementation. "extract_to_csv.py" script accesses ESMS database and aggregate stock movements to "product/facility/month" units. It should generate a file named "aggregated.csv".

## Predictive Algorithm

Current spike uses Support Vector Machine (SVM) algorithm. Run the trained model against whole training data set:
<blockquote>SVMModel, Accuracy: 71.4891%, Area under PR: 67.9026%, Area under ROC: 73.1964%</blockquote>

Run the same model against all positive data points (where/when stockout happened):
<blockquote>SVMModel, Accuracy: 77.4539%, Area under PR: 100.0000%, Area under ROC: 100.0000%</blockquote>

## Further Questions

* Overfitting - currently the training set and test set are identical. The model is likely overfit.
* More features - which other features are likely cause (or correlate to) stockout?
* Other algorithms - Linear Regression and Decision Tree algorithms are proved less effective. Maybe try Naive Bayesian algorithm? Maybe try a Neural Network?
 