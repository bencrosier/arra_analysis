import pandas as pd
import numpy as np
import networkx as nx
import datetime
import pytz
import arradata as ad

#
#These are functions to add social network metrics to the dataset
#created by Andrej @aficnar
#


def add_sna_metrics(arradata):
    
    conn = ad.get_connection()
    
    sql_query = ("""
            SELECT main_instagrammedia."userId" user_id, photo_id, main_usersinphoto."userId" user_tagged
            FROM main_usersinphoto
            JOIN main_instagrammedia ON main_usersinphoto.photo_id = main_instagrammedia.id
            """)
    all_tag_data = pd.read_sql(sql_query, conn)
    
    users = list(all_tag_data['user_id'].unique())
    
    sna_col_names = ['largest_clique', 'density', 'edge_count', 'average_clustering', #'assortativity',
                 'clique_count', 'transitivity', 'connected', 'connected_components', 'biconnected', 
                 'node_connectivity', 'edge_connectivity', 'average_connectivity', 'radius', 
                 'diameter','average_shortest_path', 'isolates', 'selfies_count', 'node_count']
    
    all_sna_metrics = []

    for u in users:
        tag_data = all_tag_data[all_tag_data['user_id'] == u][['photo_id', 'user_tagged']]
        selfies_count = sum(tag_data['user_tagged'] == u)
        tag_data = tag_data[tag_data['user_tagged'] != u]
        if tag_data.shape[0] > 0:
            taggers = list(tag_data['user_tagged'].unique())
            nNodes = len(taggers) + 1
            tag_matrix = np.array([0] * nNodes * nNodes).reshape((nNodes, nNodes))
            tag_index = lambda user: taggers.index(user) + 1
            tag_data['tag_index'] = tag_data[['user_tagged']].applymap(tag_index)['user_tagged']
            photo_list = list(tag_data['photo_id'].unique())
            tag_master = [[0] + list(tag_data[tag_data['photo_id'] == p]['tag_index']) for p in photo_list]
            for t in tag_master:
                for i in range(len(t)):
                    for j in range(i + 1, len(t)):
                        tag_matrix[t[i], t[j]] = tag_matrix[t[i], t[j]] + 1
            G = nx.from_numpy_matrix(np.matrix(tag_matrix, dtype = [('weight', int)]))
    
            sna_metrics = [u,
                nx.graph_clique_number(G),
                nx.density(G),
                nx.number_of_edges(G),
                nx.average_clustering(G),
                #nx.degree_assortativity_coefficient(G),
                nx.graph_number_of_cliques(G),
                nx.transitivity(G),
                nx.is_connected(G),
                nx.number_connected_components(G),
                nx.is_biconnected(G),
                nx.node_connectivity(G),
                nx.edge_connectivity(G),
                nx.average_node_connectivity(G),
                nx.radius(G),
                nx.diameter(G),
                nx.average_shortest_path_length(G),
                len(nx.isolates(G)),
                selfies_count,
                nx.number_of_nodes(G)]
    
            all_sna_metrics.append(sna_metrics)
    
    sna_df = pd.DataFrame(all_sna_metrics, columns = ['user_id'] + sna_col_names)
    sna_df.index = sna_df['user_id']
    del sna_df.index.name
    sna_df = sna_df.drop('user_id', axis = 1)
    
    #put on social shift columns
    arradata = add_social_shift(arradata, conn, users)
    
    #sna_df = sna_df.fillna({'assortativity': 0.0})
    
    arradata = pd.merge(arradata, sna_df, left_index = True, right_index = True, how = 'left')
    
    G = nx.Graph()
    G.add_node(1)
    
    sna_trivial = [
        nx.graph_clique_number(G),
        nx.density(G),
        nx.number_of_edges(G),
        nx.average_clustering(G),
        #0.0, #nx.degree_assortativity_coefficient(G), same as before
        nx.graph_number_of_cliques(G),
        nx.transitivity(G),
        nx.is_connected(G),
        nx.number_connected_components(G),
        nx.is_biconnected(G),
        nx.node_connectivity(G),
        nx.edge_connectivity(G),
        nx.average_node_connectivity(G),
        nx.radius(G),
        nx.diameter(G),
        0,#nx.average_shortest_path_length(G), although it's formally infinite...
        len(nx.isolates(G)),
        0, # Selfies count
        nx.number_of_nodes(G)]
    
    sna_na_dict = {sna_col_names[i]:sna_trivial[i] for i in range(len(sna_trivial))}
    arradata = arradata.fillna(sna_na_dict)

    arradata['selfies_percentage'] = arradata['selfies_count'] / arradata['media']
    arradata = arradata.fillna({'selfies_percentage': 0})

    sna_loc_start = list(arradata.columns).index('largest_clique')
    sna_loc_end = arradata.shape[1]
    
    trivial_cols_index = []
    for i in range(sna_loc_start, sna_loc_end):
        if len(arradata.ix[:, i].unique()) == 1: trivial_cols_index.append(i)
    trivial_cols = [list(arradata.columns)[index] for index in trivial_cols_index]
    
    ad_cleaned = arradata.drop(trivial_cols, axis = 1)

    sna_corr = ad_cleaned.ix[:, sna_loc_start:].corr()
    
    corrs = []
    for i in range(sna_corr.shape[0]):
        ones = sna_corr.ix[i][sna_corr.ix[i] == 1.0].index.tolist()
        ones_ind = [sna_corr.columns.tolist().index(o) for o in ones]
        if (len(ones_ind) > 1) and ones_ind not in corrs: corrs.append(ones_ind)
    corrs_labels = [[sna_corr.columns.tolist()[a] for a in c] for c in corrs]
    
    for l in corrs_labels:
        ad_cleaned = ad_cleaned.drop(l[1:], axis = 1)
    
    return ad_cleaned


#
#Add social shift data
#

def add_social_shift(arradata, conn, users):
    
    f_rate = 1. / 30.
    
    sql_query = ("""
            SELECT main_instagrammedia."userId" user_id, main_instagrammedia."createdTime" created_time
            FROM main_usersinphoto
            JOIN main_instagrammedia ON main_usersinphoto.photo_id = main_instagrammedia.id
            """)
    all_tag_data = pd.read_sql(sql_query, conn)
    
    sql_query = (""" 
             SELECT  main_user."userId" user_id, main_survey."createDate" create_date
             FROM main_survey 
             JOIN main_user
             ON main_user.survey_id = main_survey.id
             """)
    all_survey_times = pd.read_sql(sql_query, conn)
    
    utc_tz = pytz.timezone("UTC")
    cnt_tz = pytz.timezone("US/Central")
    
    def get_days_delta(timestamp):
        utc_time = datetime.datetime.utcfromtimestamp(timestamp)
        cnt_time = utc_tz.localize(utc_time).astimezone(cnt_tz)
        delta = cnt_time - first_tag_time
        return delta.days
    
    all_deg_res = []
    for u in users:
        tag_data = all_tag_data[all_tag_data['user_id'] == u]
        first_tag_timestamp = tag_data['created_time'].min()
        utc_time = datetime.datetime.utcfromtimestamp(first_tag_timestamp)
        first_tag_time = utc_tz.localize(utc_time).astimezone(cnt_tz)
        tag_delta = tag_data[['created_time']].applymap(get_days_delta)['created_time']
        survey_time = all_survey_times[all_survey_times['user_id'] == u]['create_date'][0]
        survey_time = survey_time.to_datetime().astimezone(cnt_tz)
        t_end = (survey_time - first_tag_time).days
        t_start = t_end - 365
        all_tag_days = tag_delta.unique()
        tag_days = all_tag_days[all_tag_days >= t_start]
     
        deg_list = []
        relevant_tags = tag_delta[tag_delta <= t_start]
        for t in range(t_start, t_end + 1):
            if t in tag_days:
                relevant_tags = tag_delta[tag_delta <= t]
            deg_local = sum(np.exp(- (t - relevant_tags) * f_rate))
            deg_list.append(deg_local)
    
        deg_res = [u, np.mean(deg_list), np.std(deg_list)]
        all_deg_res.append(deg_res)      
        
    tag_df = pd.DataFrame(all_deg_res, columns = ['user_id', 'deg_mean', 'deg_std'])
    tag_df.index = tag_df['user_id']
    del tag_df.index.name
    tag_df = tag_df.drop('user_id', axis = 1)
    
    arradata = pd.merge(arradata, tag_df, left_index = True, right_index = True, how = 'left')
    
    arradata['log_rel_deg'] = np.log(arradata['deg_std'] / arradata['deg_mean'] + 1)
    arradata = arradata.fillna({'deg_mean': 0, 'deg_std': 0, 'log_rel_deg': 0})
    arradata['log_deg_mean'] = np.log(arradata['deg_mean'] + 1)
    arradata['log_deg_std'] = np.log(arradata['deg_std'] + 1)
    
    return arradata