from sklearn.preprocessing import OneHotEncoder, LabelEncoder
import numpy as np

def convert_columns(arradata):
    cols_to_convert = []
    for col in list(arradata):
        print arradata[col].nunique(), arradata[col].dtype, col
        if arradata[col].dtype in ["object","bool"] or "phq" in col:
            cols_to_convert.append(col)
    
    le = LabelEncoder()
    enc = OneHotEncoder(handle_unknown="ignore")
    
    for col in cols_to_convert:
        arradata[col] = arradata[col].fillna('N/A').astype('string')
        le.fit(arradata[col])
        labels = list(le.classes_)
        transformed_classes = le.transform(arradata[col])
    
        one_hot_matrix = enc.fit_transform(transformed_classes.reshape((len(transformed_classes),1))).toarray()

        for i_col in range(0, np.shape(one_hot_matrix)[1]):
            arradata.insert(list(arradata).index(col)+i_col, col+"_"+str(labels[i_col]), one_hot_matrix[:,i_col])
    
        del arradata[col]
        
    return arradata

