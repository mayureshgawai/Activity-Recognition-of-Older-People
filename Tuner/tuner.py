import pandas as pd
from xgboost import XGBClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.svm import SVC
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import roc_auc_score, accuracy_score
import numpy as np

class Model_Finder:
    def __init__(self, file_object, logger):
        self.file_object = file_object
        self.logger = logger

    def get_best_params_xgboost(self, X_train, y_train):

        try:
            params = {
                'learning_rate': [0.5, 0.1, 0.01, 0.001],
                'max_depth': [3, 5, 10, 20],
                'n_estimators': [10, 50, 100, 200]
            }

            grid = GridSearchCV(XGBClassifier(objective='binary:logistic'), params, cv=5)
            grid.fit(X_train, y_train)

            learning_rate = grid.best_params_['learning_rate']
            max_depth = grid.best_params_['max_depth']
            n_estimators = grid.best_params_['n_estimators']

            xgb = XGBClassifier(learning_rate=learning_rate, max_depth=max_depth, n_estimators=n_estimators)
            un = y_train.unique()
            xgb.fit(X_train, y_train)
            return xgb
        except Exception as e:
            self.logger.log(self.file_object, "get_best_params_xgboost:: Error occcured while finding best params for training data for xgboot, "+str(e))


    # def find_ccpAlpha_for_dt(self, dt_model, X_train, y_train):
    #     path = dt_model.cost_complexity_pruning_path(X_train, y_train)
    #     ccp_alpha = path['ccp_alphas']
    #
    #     dt_models = []
    #     for ccp in ccp_alpha:
    #         dt = DecisionTreeClassifier(ccp_alpha=ccp, random_state=10)
    #         dt_models.append(dt)
    #
    #



    def get_best_params_dt(self, X_train, y_train):
        try:
            params = {
                'criterion': ['gini', 'entropy'],
                'splitter': ['best', 'random'],
                'max_depth': range(2, 40, 1),
                'min_samples_split': range(2, 10, 1),
                'min_samples_leaf': range(1, 10, 1)
            }

            grid = GridSearchCV(DecisionTreeClassifier() , param_grid=params, cv=5, )
            # grid.fit(X_train, y_train)

            # criterion = grid.best_params_['criterion']
            # splitter = grid.best_params_['splitter']
            # max_depth = grid.best_params_['max_depth']
            # min_samples_split = grid.best_params_['min_samples_split']
            # min_samples_leaf = grid.best_params_['min_samples_leaf']

            criterion = 'entropy'
            splitter = 'best'
            max_depth = 33
            min_samples_split = 2
            min_samples_leaf = 1

            dtree = DecisionTreeClassifier(criterion=criterion, splitter=splitter, max_depth=max_depth,
                                           min_samples_leaf=min_samples_leaf, min_samples_split=min_samples_split)
            dtree.fit(X_train, y_train)
            return dtree
        except Exception as e:
            self.logger.log(self.file_object, "get_best_params_dt:: Error occcured while finding best params for training data for DecisionTree, "+str(e))

    def get_best_params_svc(self, X_train, y_train):
        try:
            param = {
                'kernel': ['linear', 'poly', 'rbf', 'sigmoid'],
                'C': [.1, .4, .6, 1, 2, 3, 100, 200, 500],
                'gamma': [.001, .1, .4, .004, .003]
            }

            grid = GridSearchCV(SVC(), param_grid=param, cv=5 )
            # grid.fit(X_train, y_train)

            # kernel = grid.best_params_['kernel']
            # c = grid.best_params_['C']
            # gamma = grid.best_params_['gamma']
            kernel = 'poly'
            gamma = 0.001
            c = 0.1
            svc = SVC(kernel=kernel, C=c, gamma=gamma, probability=True)
            svc.fit(X_train, y_train)

            return svc
        except Exception as e:
            self.logger.log(self.file_object,
                            "get_best_params_svc:: Error occcured while finding best params for training data for SVC, " + str(
                                e))



    def get_best_model(self, X_train, X_test, y_train, y_test):

        try:
            un = y_test.unique()
            dt = self.get_best_params_dt(X_train, y_train)
            y_pred_dt = dt.predict_proba(X_test)
            # y_pred_dt = pd.Series(y_pred_dt)

            if (len(y_test.unique()) == 1):
                dt_score = accuracy_score(y_test, y_pred_dt)
                self.logger.log(self.file_object, "get_best_model:: Accuracy Score: " + str(dt_score))
            else:
                dt_score = roc_auc_score(y_test, y_pred_dt, multi_class='ovr')
                self.logger.log(self.file_object, "get_best_model:: AUC Score: " + str(dt_score))

            svc = self.get_best_params_svc(X_train, y_train)
            # y_pred_svc = svc.predict(X_test)
            # y_pred_svc = pd.Series(y_pred_svc)
            y_pred_svc = svc.predict_proba(X_test)

            if (len(y_test.unique()) == 1):
                svc_score = accuracy_score(y_test, y_pred_svc)
                self.logger.log(self.file_object, "get_best_model:: Accuracy Score: " + str(svc_score))
            else:
                svc_score = roc_auc_score(y_test, y_pred_svc, multi_class='ovr')
                self.logger.log(self.file_object, "get_best_model:: AUC Score: " + str(svc_score))

            if(dt_score > svc_score):
                return 'DecisionTree', dt
            else:
                return 'SVC', svc

        except Exception as e:
            self.logger.log(self.file_object, "get_best_model::Error Occurred in process of getting best model, "+str(e))

