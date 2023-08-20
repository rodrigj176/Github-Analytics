import requests
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
import datetime
from datetime import datetime, timedelta


def aggregate_avg(new_values, total_sum):
    return (sum(new_values) + (total_sum or 0))

def aggregate_count(new_values, total_sum):
    return (sum(new_values) + (total_sum or 0))

def aggregate_repos(new_vals, vals_list):
    return (sum(new_vals) + (vals_list or 0))

def aggregate_words(new_vals, vals_list):
    print(new_vals, vals_list)
    list = []
    if vals_list is not None:
        vals_list = vals_list + new_vals
    else:
        list.append(vals_list)

    return ((vals_list or list))
    

def aggregate_wc(new_vals, total_sum):
        return (sum(new_vals) + (total_sum or 0))

def resetr(new_vals, vals_list):
    return (0)

glob_previous = [0, 0, 0]
glob_previous[0] = 0
glob_previous[1] = 0
glob_previous[2] = 0


glob_batch = 0


def send_df_to_dashboard(df):
    url = 'http://webapp:5000/updateData'
    data = df.toPandas().to_dict('list')

    global glob_batch
    global glob_previous

    if glob_batch == 0:
        glob_previous[0] = 0
        glob_previous[1] = 0
        glob_previous[2] = 0

    print('Batch #:', glob_batch )
    
    
    try:
        print('Python:collected repo :',data['Total_Repos'][0], ' average number of stars :', round((data['Total_stars'][0]/data['Total_Repos'][0]), 2))
        data['recent_repos'][0] -= glob_previous[0]
        print('Python recent pushes:',  data['recent_repos'][0] )
        glob_previous[0] += data['recent_repos'][0]
    except IndexError:
        pass
    
    try:
        print('Ruby:collected repo :',data['Total_Repos'][1], ' average number of stars :', round((data['Total_stars'][1]/data['Total_Repos'][1]), 2))
        data['recent_repos'][1] -= glob_previous[1]
        print('Ruby recent pushes:',  data['recent_repos'][1] )
        glob_previous[1] += data['recent_repos'][1]
    except IndexError:
        pass
    
    try:
        print('PHP:collected repo :',data['Total_Repos'][2], ' average number of stars :', round((data['Total_stars'][2]/data['Total_Repos'][2]), 2))
        data['recent_repos'][2] -= glob_previous[2]
        print('PHP recent pushes:',  data['recent_repos'][2] )
        glob_previous[2] += data['recent_repos'][2]
    except IndexError:
        pass

    glob_batch = glob_batch + 1

    requests.post(url, json=data)


def get_sql_context_instance(spark_context):
    if('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SparkSession(spark_context)   
    return globals()['sqlContextSingletonInstance']


def process_rdd(time, rdd):
    pass
    print("----------- %s -----------" % str(time))
    try:
        df = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(Programming_Language=w[0], Total_stars=w[1][0][0], Total_Repos=w[1][0][1], recent_repos=w[1][1], ))
        results_df = df.createDataFrame(row_rdd)
        results_df.show()
        results_df.createOrReplaceTempView("results")
        new_results_df = df.sql("select * from results ")
        print(time)
        send_df_to_dashboard(new_results_df)


    except ValueError:
        print("Waiting for data...")


if __name__ == "__main__":
    DATA_SOURCE_IP = "data-source"
    DATA_SOURCE_PORT = 9999
    sc = SparkContext(appName="GitRepoInfo")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 60)
    ssc.checkpoint("checkpoint_GitRepoInfo")

    
    data = ssc.socketTextStream(DATA_SOURCE_IP, DATA_SOURCE_PORT)

    datas = data.map(lambda line: line.split(" "))
    
#stars
    avg_stars = datas.map(lambda x: (x[1], int(x[0]))  )  
  
    aggregated_stars = avg_stars.updateStateByKey(aggregate_avg)
    
    

#total_repo
    repo_count = datas.map(lambda x: ((x[1]), 1))
    
    aggregated_count = repo_count.updateStateByKey(aggregate_count)
   

#recent_repo 
    
    recent_repos = datas.map(lambda x: (x[1], (1 if datetime.strptime(x[2], '%Y-%m-%d|%H:%M:%S') > datetime.now() - timedelta(seconds=60) else 0))  )
    
    aggregated_repos = recent_repos.updateStateByKey(aggregate_repos)


#joining tables
    joined = aggregated_stars.join(aggregated_count)
    
    joined = joined.join(aggregated_repos)

    joined.foreachRDD(process_rdd)


#Wordcount *INCOMPLETE*

    word_count = datas.map(lambda x: (((x[1]), ((x[3].split("-")[0]),1)))) 
    

    word_df = word_count.map(lambda w: (w[0], (w[1][0], w[1][1])) )
                                        
 

    ssc.start()
    ssc.awaitTermination()