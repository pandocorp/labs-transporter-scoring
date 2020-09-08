
import pandas as pd
import numpy as np
from pandas import read_csv,DataFrame,concat,Series
from datetime import datetime,timedelta
from geopy.distance import geodesic
import holidays
import boto3
#from tensorflow.keras.models import load_model
import os
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import LabelEncoder,MinMaxScaler
from sklearn.ensemble import RandomForestRegressor 
import pickle
import warnings
import operator
from collections import defaultdict
 
import pickle
import requests
import warnings
warnings.filterwarnings("ignore")


my_path=os.getcwd()

diry='/'
for i,j in enumerate(my_path.split('/')[1:-1]):
    diry=diry+str(j)
    if i!=len( my_path.split('/')[1:-1] )-1:
        diry=diry+'/'

#diry='/home/ubuntu/Models/'

data_path=os.path.join(diry,'Data_Lane_VT_Monthly')
support_file_path=os.path.join(diry,'Transporter_scoring_Combined','SupportFiles') 
processed_datafile_path=os.path.join(diry,'Transporter_scoring_Combined','DataFiles')
model_file_path=os.path.join(diry,'Transporter_scoring_Combined','ModelFiles')
model_out_path=os.path.join(diry,'Transporter_scoring_Combined','OutputFiles')

 
#/home/ubuntu/Models/Transporter_scoring_Combined
working_columns=['Quantity',
'Arrival Breached At',
'Vehicle Type',
'contract_start_date',
'contract_end_date',
'Indent ID',
'Source_Depot_City',
'Arrived At',
'Dispatched At',
'sla_delay_charges',
'contract_source',
'depot_lat_long',
'Gross Weight',
'Transit Time',
'Delivery Date',
'Customer',
'contract_destination',
'contract_type',
'Transporter',
'Created Date',
'Base Freight',
'consignee_lat_long',
'Actual Freight',
'distance',
'carton_damage_charges',
'damage_charges',
'consignee_pincode',
'contract_id',
'Destination',
'Indent Type',
'contract_validity',
'Gross Volume',
'shortage_charges']


# In[115]:


class DataCorrectImputeUpdate:   
    
    def data_pull():
        df_history=DataFrame()
        start='2019-01-01'
        updated_time=(datetime.strptime(start,'%Y-%m-%d'))
        now = datetime.now()
        today_date=(pd.to_datetime(now.strftime("%Y-%m-%d %H:%M:%S")))
        last_month=datetime.strptime(str(today_date.year)+'-'+str(today_date.month)+'-01','%Y-%m-%d')

        while updated_time <last_month: #pd.to_datetime(datetime.now().strftime("%Y-%m-%d")):


            if (updated_time.month==1):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(31-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(31-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(31-1)




            if (updated_time.month==2) and (updated_time.year%4==0) :
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(29-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(29-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(29-1)

            if (updated_time.month==2) and (updated_time.year%4!=0) :
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(28-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(28-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(28-1)


            if (updated_time.month==3):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(31-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(31-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(31-1)



            if (updated_time.month==4):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(30-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(30-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(30-1)



            if (updated_time.month==5):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(31-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(31-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(31-1)




            if (updated_time.month==6):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(30-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(30-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(30-1)



            if (updated_time.month==7):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(31-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(31-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(31-1)




            if (updated_time.month==8):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(31-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(31-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(31-1)





            if (updated_time.month==9):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(30-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(30-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(30-1)




            if (updated_time.month==10):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(31-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(31-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(31-1)




            if (updated_time.month==11):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(30-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(30-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(30-1)




            if (updated_time.month==12):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(31-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(31-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(31-1)
                


            dfj = None

            try:
                dfj = pd.read_json(json_out_file, convert_dates=True)[working_columns]
            except ValueError:
                print(json_out_file)
                try:
                    dataset_url_check= 'http://35.154.192.224:8090/lane_vt_report?from_date=2019-07-01&to_date=2019-07-02'
                    response_check = requests.post(dataset_url_check, data={},timeout=10)
                    response = requests.post(dataset_url, data={})
                    if response.status_code == 200:
                        try:
                            #save Dataset to json
                            file = open(json_out_file, "w")
                            file.write(response.text) 
                            file.close() 
                        except OSError as err:
                            print("OS error: {0}".format(err))

                        #read json into dataframe
                        try:
                            dfj = pd.read_json(json_out_file, convert_dates=True)[working_columns]
                        
                        except ValueError:
                            print ('File not found:', json_out_file)
                            
                except:
                    print('Database Connection Error: Unable To Write New Data')

            
            df_history=df_history.append(dfj)


            updated_time=updated_time+timedelta(1)


        return df_history
        
        

    def Customer_name_correction(df):
        
        CustomerMap = {'Signify Innovations India Limited':'Signify',
                       'Marico Limited':'Marico',
                       'Orient Electric Ltd':'OEL'}
        df['Customer'] = df['Customer'].apply(lambda x: CustomerMap[x] if x in CustomerMap.keys() else x)
        df['Customer'] = df['Customer'].apply(lambda x: x.strip().capitalize())

        return df
    
    def Transporter_name_correction(df):
        df=df.dropna(subset=['Transporter'])
        
        TransMap = {'A.D. ROADLINES (INDIA) REGD.': 'A.D. Roadlines (INDIA) Regd',
                    'ABIZER CARRIERS':'Abizer Carriers',
                    'ANAND ROADLINES':'ANAND ROAD LINES',
                    'Anand Roadlines':'ANAND ROAD LINES',
                    'GATI-KINTETSU EXPRESS PVT LTD':'GATI KINTETSU EXPRESS',
                    'KAPOOR FREIGHT CARRIERS PVT LTD':'Kapoor Freight Carrier Pvt. Ltd',
                    'Om Logistics Limited':'OM Logistics',
                    'Om Logistics Ltd':'OM Logistics',
                    'OTS LIMITED':'OTS Limited',
                    'PANDEY ROAD LINES':'Pandey Roadlines',
                    'RCI LOGISTICS PVT LTD':'RCI Logistics Pvt Ltd',
                    'RIVIGO SERVICES PRIVATE LIMITED':'Rivigo Services Private Limited',
                    'SD CARGO PVT. LTD.':'SD CARGO PVT LTD',
                    'Sri Ramadas Motor Transpo':'Sri Ramadas Motor Transport Limited',
                    'STELLAR INNOVATIVE TRANSPORTATION':'Stellar Innovative Transportation',
                    'WHEELSEYE TECHNOLOGY INDIA PRIVATE LIMITED':'WHEELSEYE TECHNOLOGY INDIA PVT',
                    'ZINKA LOGISTICS SOLUTIONS PRIVATE':'ZINKA LOGISTICS SOLUTIONS',
                    'MIDDLETON LOGISTIC SOLUTIONS':'MIDDLETON LOGISTICS SOLUTIONS',
                    'SPOTON LOGISTICS PRIVATE LIMITED':'Spoton logistics Private Limited',
                    'Sri Ramadas Motor Transpo':'Sri Ramadas Motor Transport Limited',
                    'Sri Ramadas Motor Transport Ltd':'Sri Ramadas Motor Transport Limited'
        }
        
        
        df['Transporter'] = df['Transporter'].apply(lambda x: TransMap[x] if x in TransMap.keys() else x)
        df['Transporter'] = df['Transporter'].apply(lambda x: x.strip().capitalize())

        return df
        
        
    def sourcemap(df):    
        SourceMap = {
        'Guwahati (t)': 'Guwahati',
        'Jaipur-rajasthan': 'Jaipur',
        'Mil sc -ameya food': 'Coimbatore',
        'Mil- guwahati plant' : 'Guwahati',    
        'Mil- jalgaon plants' : 'Jalgaon',
        'Mil- kanjikode plant' : 'Kanjikode',
        'Mil- pondy plants' : 'Puducherry',
        'Tirumala-hyderabad' : 'Tirumala',
        'R598-coimbatore' : 'Coimbatore',
        'Snqz - khopoli' : 'Khopoli',
        'Snrq' : 'Coimbatore',
        'Jaipur-Rajasthan': 'Jaipur'
        }

        df['Source_Depot_City'] = df['Source_Depot_City'].apply(lambda x: x.strip().capitalize())
        df['Source_Depot_City'] = df['Source_Depot_City'].apply(lambda x: x.replace(',', '_'))
        df['Source'] = df['Source_Depot_City'].apply(lambda x: SourceMap[x] if x in SourceMap.keys() else x)
        df=df.drop(['Source_Depot_City'],1)
        return df

    def destmap(df):    
        DestMap = {
        'Balaosre': 'Balasore',
        'Zirakhpur': 'Zirakpur',
        'Dayalpura sodhian': 'Zirakpur',
        'Mubarikpur camp': 'Zirakpur',
        'Bhubaneswar': 'Bhubaneshwar',
        'Burdge town': 'Midnapore',
        'Chhoto mathkatpur': 'Kharagpur',
        'Vijayawada': 'Vijaywada',
        'Una-himachal pradesh': 'Una',
        'Guwahati (t)' : 'Guwahati',
        'Panskura town' : 'Panskura',
        'Rampura phul' : 'Kapurthala',
        'Raipur pachimbar': 'Contai',
        'Edapalayam , chennai': 'Chennai',
        'Kappalur , madurai': 'Madurai',
        'T c balam, vanur taluk': 'Puducherry',
        'Annur taluk, coimbatore': 'Coimbatore',
        'Renigunta mandal, chittoor dt': 'Renigunta',
        'Thane - bhiwandi.' : 'Thane',
        'Ramji mandir chowk': 'Sambalpur',
        'Village daowra (hawrah)': 'Amta',
        'Virar east, dist - palghar': 'Virar',
        'Sankrail, nh-6, howrah': 'Sankrail',
        'Quthbullapur, ida jeedimelta': 'Secunderabad',
        'Kanjikuzhy_ kottayam': 'kottayam',
        'Bestan,surat,gujarat': 'Surat',
        'Viralimalai (pudukkotai dt.)': 'Viralimalai',
        'Paramathi velur-(po), namakkal-(dis': 'Namakkal',
        'Valliyoor (kkdt)': 'Valliyoor',
        'Ongole , prakasam': 'Ongole',
        'Pallipalayam , namakkal': 'Namakkal',
        'Sheoganj dist-sirohi': 'Sirohi',
        'Barani, bhagalpur': 'Bhagalpur',
        'Pipraich ((gorakhpur)': 'Gorakhpur',
        'Titlagarh dist-bolangir': 'Balangir',
        'Ramnagar, varanasi': 'Varanasi',
        'Fhullbari -siliguri': 'Siliguri',
        'Mavelikara , kerala': 'Mavelikara',
        'Langtlai-mizoram': 'Langtlai',
        'Kavundampalayam_ coimbatore': 'Coimbatore',
        '24 pgs (north)': 'Habra',
        '24 parganas(south)': 'Dakshin Barasat',
        'Chengannur_ kerala': 'Chengannur',
        '846004drabhanga': 'Darbhanga',
        "'jhajjar":'Jhajjar',
        'Allahaabad':'Allahabad',
        'Allahabad (fafamau)':'Allahabad',
        "'karimganj":'Karimganj'}
        
        df['Destination'] = df['Destination'].apply(lambda x: x.strip().capitalize())
        df['Destination'] = df['Destination'].apply(lambda x: DestMap[x] if x in DestMap.keys() else x)
        df['Destination'] = df['Destination'].apply(lambda x: x.replace(',', '_'))
        df = df[~df['Destination'].str.contains('^\d+')]  # drop 110006 etc 
        return df

    
    def imp_dist(df):
        dfj=df.copy()
        dfj=dfj[(dfj.Customer!='Sandbox')]
        dfj['distance']=dfj['distance'].apply(lambda x: np.nan if x<=0 else x)
        dfj['distance']=dfj['distance'].fillna(0)
        dfj['distance']=dfj.apply(lambda x:int(geodesic((x['Source Lat'],x['Source Long']),
                                                    (x['Dest Lat'],x['Dest Long'])).km) if x['distance']==0 else int(x['distance']) ,axis=1)

        return dfj 

    
    def lat_lon_prep(df):
    
        df.dropna(subset = ["depot_lat_long"], inplace=True)
 
        df.dropna(subset = ["consignee_lat_long"], inplace=True)

        df['Source Lat'] = df['depot_lat_long'].apply (lambda x: float(x.split(',')[0]) if float(x.split(',')[0])!=0 else np.nan)  
        df['Source Long'] = df['depot_lat_long'].apply (lambda x: float(x.split(',')[1])if float(x.split(',')[0])!=0 else np.nan)
        df['Dest Lat'] = df['consignee_lat_long'].apply (lambda x: float(x.split(',')[0])if float(x.split(',')[0])!=0 else np.nan)
        df['Dest Long'] = df['consignee_lat_long'].apply (lambda x: float(x.split(',')[1])if float(x.split(',')[0])!=0 else np.nan)
        
        df = df.drop(['depot_lat_long', 'consignee_lat_long'], axis='columns')
        df.dropna(subset=['Source Lat'])
        df.dropna(subset=['Source Long'])
        df.dropna(subset=['Dest Lat'])
        df.dropna(subset=['Dest Long'])
        df=df[(df['Source Lat'] > 8)&
                (df['Source Lat'] <32)&
                (df['Source Long']> 67)&
                (df['Source Long']<97)&
                (df['Dest Lat']>8)&
                (df['Dest Lat']<32)&
                (df['Dest Long']>67)&
                (df['Dest Long']<97)]
        df=df.drop_duplicates()
        df=df.reset_index().drop(['index'],1)
        return df

    def date_format(date):

        try:
            date_time=datetime.strptime(date.split('.')[0],'%Y-%m-%d %H:%M:%S' )
        except:
            pass
        try:
            date_time=datetime.strptime(date.split('+')[0],'%Y-%m-%d %H:%M:%S' )
        except:
            pass

        return date_time+timedelta(hours=5.5)    
    



    def update_hist_data(hist_data):
        data_from=str(max((hist_data['Created Date'].apply(lambda x:DataCorrectImputeUpdate.date_format(x))))+timedelta(1)).split(' ')[0]
        print('Fetching Data\nFrom:',data_from)
        now = datetime.now()
        today_date=(pd.to_datetime(now.strftime("%Y-%m-%d %H:%M:%S")))
        data_to=str(today_date-timedelta(1)).split(' ')[0]

        print('To:' ,data_to)
        c=-3
        try:
            dataset_url= 'http://35.154.192.224:8090/lane_vt_report?from_date=2019-07-01&to_date=2019-07-02'
            response = requests.post(dataset_url, data={},timeout=20)
        except:
            c=-2
            
        if c!=-2:
            try:
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(data_from,data_to)
                print(dataset_url)
                response = requests.post(dataset_url, data={})
            except:
                c=-1
                print('Database Connection Error: Unable To Update the Data')
            if c!=-1:
                try:
                    dfj=DataFrame(response.json())[working_columns]

                except:
                    c=0
                    print('Empty dataframe from Database')
                if c!=0:
                    try:
                        dfj=DataCorrectImputeUpdate.sourcemap(dfj)
                    except:
                        c=1
                        print('Correction Error: Source Map')
                    if c!=1:
                        try:
                            dfj=DataCorrectImputeUpdate.destmap(dfj)
                        except:
                            c=2
                            print('Correction Error: Destination Map')
                        if c!=2:
                            try:
                                dfj=DataCorrectImputeUpdate.Customer_name_correction(dfj) 
                            except:
                                c=3
                                print('Correction Error: Customer Name ')
                            if c!=3:
                                try:
                                    dfj=DataCorrectImputeUpdate.Transporter_name_correction(dfj)

                                except:
                                    c=4
                                    print('Correction Error: Transporter Name ')
                                if c!=4:
                                    try:
                                        dfj=DataCorrectImputeUpdate.lat_lon_prep(dfj)
                                        hist_data=hist_data.append(dfj)
                                        c=5
                                    except:
                                        print('Correction Error: Lat Long')

                                    
                                    
        if c==5:
            print('Data Sucessfully Updated')
        if c!=5:
            print('Unable to Update the Data')#raise ValueError('Unable to Update Data')
            

                            
                

        return hist_data
    


# In[7]:


## write monthly data in Data_Lane_VT_Monthly Folder
hist_data=DataCorrectImputeUpdate.data_pull()

## check for availability of history data, if available update it else work with old data
try:
    hist_data=pd.read_csv(os.path.join(processed_datafile_path,'history_data.csv'))
    hist_data=DataCorrectImputeUpdate.update_hist_data(hist_data)
    hist_data.to_csv(os.path.join(processed_datafile_path,'history_data.csv'),index=False)
except:
    dfj=DataCorrectImputeUpdate.sourcemap(hist_data)
    dfj=DataCorrectImputeUpdate.destmap(dfj)
    dfj=DataCorrectImputeUpdate.Customer_name_correction(dfj)   
    dfj=DataCorrectImputeUpdate.Transporter_name_correction(dfj)
    dfj=DataCorrectImputeUpdate.lat_lon_prep(dfj)
    dfj=DataCorrectImputeUpdate.imp_dist(dfj)
    dfj.to_csv(os.path.join(processed_datafile_path,'history_data.csv'),index=False)
    hist_data=pd.read_csv(os.path.join(processed_datafile_path,'history_data.csv'))
    hist_data=DataCorrectImputeUpdate.update_hist_data(hist_data)
    hist_data.to_csv(os.path.join(processed_datafile_path,'history_data.csv'),index=False)
    


# In[8]:


hist_data=pd.read_csv(os.path.join(processed_datafile_path,'history_data.csv'))


class Common_Features:
    
    def correctdtypes(df):
        for i in df.columns:
            if operator.contains(i,'month') or operator.contains(i,'weekday') or operator.contains(i,'hour')or operator.contains(i,'day') or operator.contains(i,'year'):
                df[i+'_continous']=df[i].astype('int32')
                df[i]=df[i].astype(str)
            if operator.contains(i,'Target') or operator.contains(i,'distance') or operator.contains(i,'Transit Time'):
                df[i]=df[i].astype('int32')
            if operator.contains(i,'Lat') or operator.contains(i,'Long'):
                df[i]=df[i].astype('float32')

        return df



    def hdays(df,column_name):
        holiday_date=[]

        holis = holidays.CountryHoliday('IND')
        for i,j in holis.items():
             holiday_date.append(i)
        list_holidays=Series([holis.get(i)  for i in df[column_name]])
        list_holidays=Series([i if i !=None else 'Normal' for i in list_holidays])
        df['holi']=list_holidays
        return df


    def row_to_col(df,column_name):
        list_df=[]
        df_dict={}
        for i in range(len(df.columns)):
            for i in df.iloc[:,i].values:
                list_df.append(i)
        df_dict[column_name]=list_df
        return pd.DataFrame(df_dict)

    def calender_days(df,input_column_name,output_column_name):
        df['{}month'.format(output_column_name)]=df[input_column_name].dt.month
        df['{}year'.format(output_column_name)]=df[input_column_name].dt.year
        df['{}hour'.format(output_column_name)]=df[input_column_name].dt.hour
        df['{}weekday'.format(output_column_name)]=df[input_column_name].dt.weekday
        df['{}day'.format(output_column_name)]=df[input_column_name].dt.day
        return df

    def lag_var(data, n_in=1, n_out=1, dropnan=True):
        n_vars = 1 if type(data) is list else data.shape[1]
        columns_df=data.columns
        df = DataFrame(data)
        cols, names = list(), list()

        for i in (range(n_in, 0, -1)):
            cols.append(df.shift(i))
            names += [(k+'_var(t-%d)' % (i)) for j,k in zip(range(n_vars),columns_df)]

        agg = concat(cols, axis=1)
        agg.columns = names

        if dropnan:
            agg.dropna(inplace=True)
        return agg




class Placement:
    
    def select_columns(hist_data):
        
        df_data=hist_data.drop(['Quantity',
                          #'Arrival Breached At',
                          #'Vehicle Type',
                          'contract_start_date',
                          'contract_end_date',
                          #'Indent ID',
                          #'Arrived At',
                          'Dispatched At',
                          'sla_delay_charges',
                          'contract_source',
                          #'Source Lat',
                          #'Source Long',
                          #'Dest Lat',
                          #'Dest Long',
                          #'Gross Weight',
                          #'Transit Time',
                          'Delivery Date',
                          #'Customer',
                          'contract_destination',
                          #'contract_type',
                          #'Transporter',
                          #'Created Date',
                          #'Base Freight',
                          #'Actual Freight',
                          #'distance',
                          'carton_damage_charges',
                          'damage_charges',
                          'consignee_pincode',
                          'contract_id',
                          #'Destination',
                          #'Indent Type',
                          'contract_validity',
                          'Gross Volume',
                          'shortage_charges',
                          #'Source'
                         ],1)
    

        return df_data
    
    
    def data_prep_ml(df_history):
        
        df_history['Arrival Breached At']=df_history['Arrival Breached At'].fillna(0)
        df_history['Target']=df_history['Arrival Breached At'].apply(lambda x : 1 if x!=0 else x)
        df_history=df_history.drop(['Arrival Breached At'],1)

        
        df_history=df_history[~df_history.isna().any(axis=1)]
        df_history=df_history[(df_history.iloc[:, 0:] != 'NaT').all(axis=1)]
        df_history= df_history[df_history['Transporter'] != 'Dummy']
        df_history['Transporter']=df_history['Transporter'].apply(lambda x: x.strip().capitalize())
        df_history = df_history[~df_history['Destination'].str.contains('^\d+')]
        
        for j in ['Created Date','Arrived At'] :
            #print(j)
            df_history[j]=df_history[j].apply(lambda x:DataCorrectImputeUpdate.date_format(x))

#         Create input features
 
        ## Indent Count total
        Total_indent_count=df_history.groupby(['Indent ID','Transporter']).count().reset_index().groupby(['Transporter']).count().reset_index()[['Transporter','Indent ID']]
        Total_indent_count.columns=['Transporter','Total_Indent_Count']

        ## Indent Count by route
        Indent_count_by_route=df_history.groupby(['Indent ID','Transporter','Source', 'Destination']).count().reset_index().groupby(['Transporter','Source', 'Destination']).count().reset_index()[['Transporter','Source', 'Destination','Indent ID']]
        Indent_count_by_route.columns=['Transporter','Source', 'Destination','Count_by_route']

        df_history=pd.merge(df_history,Total_indent_count, how='inner', on=None, left_on='Transporter', right_on='Transporter',
                 left_index=False, right_index=False, sort=False,
                 suffixes=('_x', '_y'), copy=True, indicator=False,
                 validate=None) 

        df_history=pd.merge(df_history,Indent_count_by_route, how='inner', on=None, left_on=['Transporter','Source', 'Destination'], right_on=['Transporter','Source', 'Destination'],
                 left_index=False, right_index=False, sort=False,
                 suffixes=('_x', '_y'), copy=True, indicator=False,
                 validate=None) 
        ## Special days
        df_history=Common_Features.hdays(df_history,'Created Date')


        ## creating calender days
        df_history=Common_Features.calender_days(df_history,'Created Date','creation')
        
        
 
        ## variable to capture recency
        try:
            df_history['Created Date']=df_history['Created Date'].apply(lambda x: DataCorrectImputeUpdate.date_format(x))
        except:
            pass
        date_string = "21 January, 2018"
        refrence_date=datetime.strptime(date_string, "%d %B, %Y")
        df_history['creation_age']=round(( df_history['Created Date']-refrence_date ).dt.total_seconds()/(3600*24))


        ## Vehicle Count
        dict_count={}
        vc=[]
        tn=[]
        for i in pd.unique(df_history['Transporter']):
            vc.append(len(pd.unique(df_history[df_history['Transporter']==i]['Vehicle Type'])))
            tn.append(i)
        dict_count['vehicle_count']=vc
        dict_count['transporter_name']=tn
        vehicle_count_df=DataFrame(dict_count)
        df_history=pd.merge(df_history,vehicle_count_df, how='inner', on=None, left_on='Transporter', right_on='transporter_name',
                 left_index=False, right_index=False, sort=False,
                 suffixes=('_x', '_y'), copy=True, indicator=False,
                 validate=None).drop(['transporter_name'],1)
        
        return df_history

    def data_prep_lag(df_data)   :
        # Lags Target
        df_arranged=df_data.sort_values(by='Created Date')


        Target_lags=10

        lags_df=Common_Features.lag_var(df_arranged[['Target']],n_in=Target_lags)
        lags_df=lags_df.reset_index().drop(['index'],1)

        df_arranged=df_arranged.iloc[Target_lags:,:]
        df_arranged=df_arranged.reset_index().drop(['index'],1)

        df_lagged=pd.concat([df_arranged,lags_df],1).dropna()
        
        # Lags transportwise
        new_df=DataFrame()
        for i in pd.unique(df_lagged['Transporter']):
            #print(i)
            df_sample=(df_lagged[df_lagged['Transporter']==i]).sort_values(by='Created Date')

            ## Auto regressive effect
            lags=10

            lags_trans=Common_Features.lag_var(df_sample[['Source','Destination','Vehicle Type','Target','creationmonth', 'creationyear','creationweekday', 'creationhour', 'creationday']],n_in=lags)
            lags_trans=lags_trans.reset_index().drop(['index'],1)
            lags_trans.columns=lags_trans.columns+['_transporter_wise']
            lags_trans=lags_trans.reset_index().drop(['index'],1)

            df_sample=df_sample.iloc[lags:,:]
            df_sample=df_sample.reset_index().drop(['index'],1)

            df_laggedd=pd.concat([df_sample,lags_trans],1).dropna()
            new_df=new_df.append(df_laggedd)

        return new_df
        
class OnTimeDelivery:
    
    def select_columns(hist_data):
        df_data=hist_data.drop(['Quantity',
                          #'Arrival Breached At',
                          #'Vehicle Type',
                          'contract_start_date',
                          'contract_end_date',
                          #'Indent ID',
                          #'Arrived At',
                          #'Dispatched At',
                          #'sla_delay_charges',
                          'contract_source',
                          #'Source Lat',
                          #'Source Long',
                          #'Dest Lat',
                          #'Dest Long',
                          #'Gross Weight',
                          #'Transit Time',
                          #'Delivery Date',
                          #'Customer',
                          'contract_destination',
                          #'contract_type',
                          #'Transporter',
                          #'Created Date',
                          #'Base Freight',
                          #'Actual Freight',
                          #'distance',
                          'carton_damage_charges',
                          'damage_charges',
                          'consignee_pincode',
                          'contract_id',
                          #'Destination',
                          #'Indent Type',
                          #'Source'
                          'contract_validity',
                          'Gross Volume',
                          'shortage_charges'],1)


        return df_data
        

    
    def data_prep_ml(df_history):        
        
        df_history['Arrival Breached At']=df_history['Arrival Breached At'].fillna(0)
        df_history['Placement_Status']=df_history['Arrival Breached At'].apply(lambda x : 1 if x!=0 else x)
        df_history=df_history.drop(['Arrival Breached At'],1)
 

 

        
        df_history=df_history[~df_history.isna().any(axis=1)]
        df_history=df_history[(df_history.iloc[:, 0:] != 'NaT').all(axis=1)]
        df_history= df_history[df_history['Transporter'] != 'Dummy']
        df_history['Transporter']=df_history['Transporter'].apply(lambda x: x.strip().capitalize())
        df_history = df_history[~df_history['Destination'].str.contains('^\d+')]
 
        
        ## Contract
        df_contract=df_history[df_history['Indent Type']=='Contract']
    
        df_contract=df_contract[df_contract['Transit Time']!=0]
        
        df_contract['Transit Time']=df_contract['Transit Time']*24
       
     
        df_contract= df_contract[df_contract['Transporter'] != 'Dummy']
       
        df_contract=df_contract[~df_contract.isna().any(axis=1)]
         
         
        df_contract['Transporter']=df_contract['Transporter'].apply(lambda x: x.strip().capitalize())
      
        df_contract = df_contract[~df_contract['Destination'].str.contains('^\d+')]
        
        df_contract=df_contract[(df_contract.iloc[:, 0:] != 'NaT').all(axis=1)]
       
                
        for j in ['Created Date','Dispatched At','Delivery Date','Arrived At'] :
            try:
                df_contract[j]=df_contract[j].apply(lambda x:DataCorrectImputeUpdate.date_format(x))
            except:
                pass
            
        df_contract['time_to_delv']=(df_contract['Delivery Date']-df_contract['Dispatched At']).dt.total_seconds()/3600
        df_contract=df_contract[df_contract.time_to_delv>0] 
                      
        
        ## Open
        df_open=df_history[df_history['Indent Type']=='Open'] 
        df_open= df_open[df_open['Transporter'] != 'Dummy']
        df_open=df_open[~df_open.isna().any(axis=1)]
         
        df_open['Transporter']=df_open['Transporter'].apply(lambda x: x.strip().capitalize())
        df_open = df_open[~df_open['Destination'].str.contains('^\d+')]
        df_open=df_open[(df_open.iloc[:, 0:] != 'NaT').all(axis=1)]
        for j in ['Created Date','Dispatched At','Delivery Date','Arrived At'] :
            try:
                df_open[j]=df_open[j].apply(lambda x:DataCorrectImputeUpdate.date_format(x))
            except:
                pass        

        
        ## Target Variable 'On time delivery Status'

        df_contract['Target']=df_contract[['Transit Time','time_to_delv']].apply(lambda x: 0 if x.time_to_delv>x['Transit Time'] else 1,axis=1)

        df_open['Target']=df_open['sla_delay_charges'].apply(lambda x: 0 if x>0 else 1)
        
        df_history=df_contract.append(df_open)
        

        
#         Create input features
 
        ## Indent Count total
        Total_indent_count=df_history.groupby(['Indent ID','Transporter']).count().reset_index().groupby(['Transporter']).count().reset_index()[['Transporter','Indent ID']]
        Total_indent_count.columns=['Transporter','Total_Indent_Count']

        ## Indent Count by route
        Indent_count_by_route=df_history.groupby(['Indent ID','Transporter','Source', 'Destination']).count().reset_index().groupby(['Transporter','Source', 'Destination']).count().reset_index()[['Transporter','Source', 'Destination','Indent ID']]
        Indent_count_by_route.columns=['Transporter','Source', 'Destination','Count_by_route']

        df_history=pd.merge(df_history,Total_indent_count, how='inner', on=None, left_on='Transporter', right_on='Transporter',
                 left_index=False, right_index=False, sort=False,
                 suffixes=('_x', '_y'), copy=True, indicator=False,
                 validate=None) 

        df_history=pd.merge(df_history,Indent_count_by_route, how='inner', on=None, left_on=['Transporter','Source', 'Destination'], right_on=['Transporter','Source', 'Destination'],
                 left_index=False, right_index=False, sort=False,
                 suffixes=('_x', '_y'), copy=True, indicator=False,
                 validate=None) 
        ## Special days
        df_history=Common_Features.hdays(df_history,'Created Date')
        df_history=Common_Features.hdays(df_history,'Dispatched At')
        df_history=Common_Features.hdays(df_history,'Delivery Date')


        ## creating calender days
        df_history=Common_Features.calender_days(df_history,'Created Date','creation')
        df_history=Common_Features.calender_days(df_history,'Dispatched At','Dispatched')
        df_history=Common_Features.calender_days(df_history,'Delivery Date','Delivery')
        
        ## variable to capture recency
 
        try:
            df_history['Created Date']=df_history['Created Date'].apply(lambda x: DataCorrectImputeUpdate.date_format(x))
        except:
            pass
        date_string = "21 January, 2018"
        refrence_date=datetime.strptime(date_string, "%d %B, %Y")
        df_history['creation_age']=round(( df_history['Created Date']-refrence_date ).dt.total_seconds()/(3600*24))

        

        ## Vehicle type Count
        dict_count={}
        vc=[]
        tn=[]
        for i in pd.unique(df_history['Transporter']):
            vc.append(len(pd.unique(df_history[df_history['Transporter']==i]['Vehicle Type'])))
            tn.append(i)
        dict_count['vehicle_count']=vc
        dict_count['transporter_name']=tn
        vehicle_count_df=DataFrame(dict_count)
        df_history=pd.merge(df_history,vehicle_count_df, how='inner', on=None, left_on='Transporter', right_on='transporter_name',
                 left_index=False, right_index=False, sort=False,
                 suffixes=('_x', '_y'), copy=True, indicator=False,
                 validate=None).drop(['transporter_name'],1)
        
        return df_history

    def data_prep_lag(df_data)   :
        # Lags Target
        df_arranged=df_data.sort_values(by='Created Date')


        Target_lags=10

        lags_df=Common_Features.lag_var(df_arranged[['Target']],n_in=Target_lags)
        lags_df=lags_df.reset_index().drop(['index'],1)

        df_arranged=df_arranged.iloc[Target_lags:,:]
        df_arranged=df_arranged.reset_index().drop(['index'],1)

        df_lagged=pd.concat([df_arranged,lags_df],1).dropna()

        #print(df_lagged['Transporter'])
        # # Crate Arrival Duration variable
        #df_lagged['arrival_duration']=((df_lagged['Arrived At']) - (df_lagged['Created Date'])).dt.total_seconds()/3600

        # Lags transportwise
        new_df=DataFrame()
        for i in pd.unique(df_lagged['Transporter']):
            #print(i)
            df_sample=(df_lagged[df_lagged['Transporter']==i]).sort_values(by='Created Date')

            ## Auto regressive effect
            lags=10

            lags_trans=Common_Features.lag_var(df_sample[['Source','Destination','Vehicle Type','Target','sla_delay_charges','creationmonth', 'creationyear','creationweekday', 'creationhour', 'creationday','Placement_Status','Deliverymonth', 'Deliveryyear','Deliveryweekday', 'Deliveryhour', 'Deliveryday','Dispatchedmonth','Dispatchedyear','Dispatchedweekday', 'Dispatchedhour', 'Dispatchedday']],n_in=lags)  
            
            lags_trans=lags_trans.reset_index().drop(['index'],1)
            lags_trans.columns=lags_trans.columns+['_transporter_wise']
            lags_trans=lags_trans.reset_index().drop(['index'],1)

            df_sample=df_sample.iloc[lags:,:]
            df_sample=df_sample.reset_index().drop(['index'],1)

            df_laggedd=pd.concat([df_sample,lags_trans],1).dropna()
            new_df=new_df.append(df_laggedd)

        return new_df
        
class DamageFreeShipment:
    
    def select_columns(hist_data):
        df_data=hist_data.drop(['Quantity',
                          #'Arrival Breached At',
                          #'Vehicle Type',
                          'contract_start_date',
                          'contract_end_date',
                          #'Indent ID',
                          #'Arrived At',
                          #'Dispatched At',
                          #'sla_delay_charges',
                          'contract_source',
                          #'Source Lat',
                          #'Source Long',
                          #'Dest Lat',
                          #'Dest Long',
                          #'Gross Weight',
                          #'Transit Time',
                          #'Delivery Date',
                          #'Customer',
                          'contract_destination',
                          #'contract_type',
                          #'Transporter',
                          #'Created Date',
                          #'Base Freight',
                          #'Actual Freight',
                          #'distance',
                          #'carton_damage_charges',
                          #'damage_charges',
                          'consignee_pincode',
                          'contract_id',
                          #'Destination',
                          #'Indent Type',
                          'contract_validity',
                          'Gross Volume',
                          #'shortage_charges',
                          #'Source'
                     ],1)
        return df_data
    
    def data_prep_ml(df_history):
        
        ## Placement Status
        df_history['Arrival Breached At']=df_history['Arrival Breached At'].fillna(0)
        df_history['Placement_Status']=df_history['Arrival Breached At'].apply(lambda x : 1 if x!=0 else x)
        df_history=df_history.drop(['Arrival Breached At'],1)
 
        df_history=df_history[~df_history.isna().any(axis=1)]
            
        df_history=df_history[(df_history.iloc[:, 0:] != 'NaT').all(axis=1)]
         
        df_history= df_history[df_history['Transporter'] != 'Dummy']
         
        df_history['Transporter']=df_history['Transporter'].apply(lambda x: x.strip().capitalize())
        df_history = df_history[~df_history['Destination'].str.contains('^\d+')]
        
        
        for j in ['Created Date','Arrived At','Dispatched At','Delivery Date'] :
            #print(j)
            df_history[j]=df_history[j].apply(lambda x:DataCorrectImputeUpdate.date_format(x))
            
        df_history['time_to_delv']=(df_history['Delivery Date']-df_history['Dispatched At']).dt.total_seconds()/3600
        
         
        df_contract=df_history[df_history['Indent Type']=='Contract']
        df_contract=df_history[df_history['Transit Time']!=0]
        df_contract['Transit Time']=df_contract['Transit Time']*24

        df_open=df_history[df_history['Indent Type']=='Open']
        
        #df_extra=df_history[~df_history['Indent Type'].isin(['Open', 'Contract'])]
        
        
        ## 'On time delivery Status'

        df_contract['OnTimeStatus']=df_contract[['Transit Time','time_to_delv']].apply(lambda x: 0 if x.time_to_delv>x['Transit Time'] else 1,axis=1)

        df_open['OnTimeStatus']=df_open['sla_delay_charges'].apply(lambda x: 0 if x>0 else 1)
        
        df_history=df_contract.append(df_open)
        
        ## Target Variable Damages
        df_history=df_history[df_history['Actual Freight']>0]
        df_history['Target']=((0.6*df_history['shortage_charges']) + (0.3*df_history['damage_charges']) + (0.1*df_history['carton_damage_charges']))/df_history['Actual Freight']
        
#         Create input features
 
        ## Indent Count total
        Total_indent_count=df_history.groupby(['Indent ID','Transporter']).count().reset_index().groupby(['Transporter']).count().reset_index()[['Transporter','Indent ID']]
        Total_indent_count.columns=['Transporter','Total_Indent_Count']

        ## Indent Count by route
        Indent_count_by_route=df_history.groupby(['Indent ID','Transporter','Source', 'Destination']).count().reset_index().groupby(['Transporter','Source', 'Destination']).count().reset_index()[['Transporter','Source', 'Destination','Indent ID']]
        Indent_count_by_route.columns=['Transporter','Source', 'Destination','Count_by_route']

        df_history=pd.merge(df_history,Total_indent_count, how='inner', on=None, left_on='Transporter', right_on='Transporter',
                 left_index=False, right_index=False, sort=False,
                 suffixes=('_x', '_y'), copy=True, indicator=False,
                 validate=None) 

        df_history=pd.merge(df_history,Indent_count_by_route, how='inner', on=None, left_on=['Transporter','Source', 'Destination'], right_on=['Transporter','Source', 'Destination'],
                 left_index=False, right_index=False, sort=False,
                 suffixes=('_x', '_y'), copy=True, indicator=False,
                 validate=None) 
        
        
        
        ## Special days
        df_history=Common_Features.hdays(df_history,'Created Date')
        df_history=Common_Features.hdays(df_history,'Dispatched At')
        df_history=Common_Features.hdays(df_history,'Delivery Date')


        ## creating calender days
        df_history=Common_Features.calender_days(df_history,'Created Date','creation')
        df_history=Common_Features.calender_days(df_history,'Created Date','Dispatched')
        df_history=Common_Features.calender_days(df_history,'Created Date','Delivery')


        ## Vehicle type Count
        dict_count={}
        vc=[]
        tn=[]
        for i in pd.unique(df_history['Transporter']):
            vc.append(len(pd.unique(df_history[df_history['Transporter']==i]['Vehicle Type'])))
            tn.append(i)
        dict_count['vehicle_count']=vc
        dict_count['transporter_name']=tn
        vehicle_count_df=DataFrame(dict_count)
        df_history=pd.merge(df_history,vehicle_count_df, how='inner', on=None, left_on='Transporter', right_on='transporter_name',
                 left_index=False, right_index=False, sort=False,
                 suffixes=('_x', '_y'), copy=True, indicator=False,
                 validate=None).drop(['transporter_name'],1)
        
        return df_history

    def data_prep_lag(df_data)   :
        # Lags Target
        df_arranged=df_data.sort_values(by='Created Date')


        Target_lags=10

        lags_df=Common_Features.lag_var(df_arranged[['Target']],n_in=Target_lags)
        lags_df=lags_df.reset_index().drop(['index'],1)

        df_arranged=df_arranged.iloc[Target_lags:,:]
        df_arranged=df_arranged.reset_index().drop(['index'],1)

        df_lagged=pd.concat([df_arranged,lags_df],1).dropna()

        #print(df_lagged['Transporter'])
        # # Crate Arrival Duration variable
        #df_lagged['arrival_duration']=((df_lagged['Arrived At']) - (df_lagged['Created Date'])).dt.total_seconds()/3600

        # Lags transportwise
        new_df=DataFrame()
        for i in pd.unique(df_lagged['Transporter']):
            #print(i)
            df_sample=(df_lagged[df_lagged['Transporter']==i]).sort_values(by='Created Date')

            ## Auto regressive effect
            lags=10

            lags_trans=Common_Features.lag_var(df_sample[['Source','Destination','Vehicle Type','Target','sla_delay_charges','creationmonth', 'creationyear','creationweekday', 'creationhour', 'creationday','Placement_Status','OnTimeStatus','Deliverymonth', 'Deliveryyear','Deliveryweekday', 'Deliveryhour', 'Deliveryday','Dispatchedmonth','Dispatchedyear','Dispatchedweekday', 'Dispatchedhour', 'Dispatchedday']],n_in=lags)  
            lags_trans=lags_trans.reset_index().drop(['index'],1)
            lags_trans.columns=lags_trans.columns+['_transporter_wise']
            lags_trans=lags_trans.reset_index().drop(['index'],1)

            df_sample=df_sample.iloc[lags:,:]
            df_sample=df_sample.reset_index().drop(['index'],1)

            df_laggedd=pd.concat([df_sample,lags_trans],1).dropna()
            new_df=new_df.append(df_laggedd)

        return new_df
        
## Machine Learning Variables Creation for Placement Probability 
df_data=Placement.select_columns(hist_data)
hist_data_ml_PP=Placement.data_prep_ml(df_data)
hist_data_ml_PP.to_csv(os.path.join(processed_datafile_path,'hist_data_ml_PP.csv'),index=False)


# In[14]:


## Machine Learning Variables Creation for Ontime Delivery Probability 
df_data=OnTimeDelivery.select_columns(hist_data)
hist_data_ml_dlv=OnTimeDelivery.data_prep_ml(df_data)
hist_data_ml_dlv.to_csv(os.path.join(processed_datafile_path,'hist_data_ml_dlv.csv'),index=False)
 


# In[15]:


## Machine Learning Variables Creation for Damage Free Shipment Prediction
df_data=DamageFreeShipment.select_columns(hist_data)
hist_data_ml_cost=DamageFreeShipment.data_prep_ml(df_data)
hist_data_ml_cost.to_csv(os.path.join(processed_datafile_path,'hist_data_ml_cost.csv'),index=False)


input_df_=hist_data.groupby(['Customer','Source','Destination','Transporter','Vehicle Type']).size().reset_index()[['Customer','Source','Destination','Transporter','Vehicle Type']].drop_duplicates()


Trans_count=input_df_.groupby(['Transporter'])['Customer'].count().reset_index()
Trans_count.columns=['Transporter','Trans_count']
input_df_=pd.merge(input_df_,Trans_count,how='inner',on=['Transporter'])
input_df_=input_df_[input_df_.Trans_count>10].drop(['Trans_count'],1)


indent_df_lanewise=hist_data[['Indent ID','Customer','Transporter','Source','Destination','Vehicle Type']].drop_duplicates(subset=['Indent ID']).groupby(['Customer','Transporter','Source','Destination','Vehicle Type'])#.count().reset_index()
indent_df_lanewise=indent_df_lanewise.count().reset_index()
indent_df_lanewise['Indent_Count_lanewise']=indent_df_lanewise['Indent ID']
indent_df_lanewise=indent_df_lanewise.drop(['Indent ID'],1)
# indent_df_lanewise[indent_df_lanewise.Customer=='Marico']


indent_df_lanewise['Transporter']=indent_df_lanewise['Transporter'].apply(lambda x: x.strip().capitalize())


indent_df=indent_df_lanewise.groupby(['Customer','Transporter']).agg({'Indent_Count_lanewise':'sum'}).reset_index()
indent_df['Total_Indent_Count']=indent_df['Indent_Count_lanewise']
indent_df=indent_df.drop(['Indent_Count_lanewise'],1)
# indent_df[indent_df.Customer=='Marico']


# In[29]:


indent_df=pd.merge(indent_df_lanewise,indent_df,on=['Customer','Transporter'],how='inner') 


# In[31]:


# indent_df


# ## Input feature creation from query and get scores

# In[32]:


class TransportersScore:
    
    def pp_scores(hist_data,input_df_):
        
        now = datetime.now()
        today_date=(pd.to_datetime(now.strftime("%Y-%m-%d %H:%M:%S")))
        input_date=(today_date)
        input_df_['Created Date']=input_date
#         input_date=max(hist_data['Created Date'].apply(lambda x: DataCorrectImputeUpdate.date_format(x)))+timedelta(days=1)
#         dtime=str(max(hist_data['Created Date'].apply(lambda x: DataCorrectImputeUpdate.date_format(x)))+timedelta(days=1)).split(' ')

#         input_date=datetime.strptime(dtime[0]+' '+'14:00:00','%Y-%m-%d %H:%M:%S')
#         input_df_['Created Date']=input_date

        
        input_df_['Transporter']=input_df_['Transporter'].apply(lambda x: x.strip().capitalize())
         
        Input_df=Common_Features.hdays(input_df_,'Created Date')
        Input_df=Common_Features.calender_days(Input_df,'Created Date','creation')
        hist_data_ml_PP=pd.read_csv(os.path.join(processed_datafile_path,'hist_data_ml_PP.csv'))
        df_help=hist_data_ml_PP.drop(['holi',
                              'Created Date',
                              'Indent ID',
                              'Arrived At',
                              'Target',
                             'creationmonth',
                             'creationyear',
                             'creationhour',
                             'creationweekday',
                             'creationday',
                             'creation_age'],1)

        Input_1=pd.merge(Input_df,df_help, how='inner', on=None,
                         left_on=['Customer','Source','Destination','Transporter','Vehicle Type'],
                         right_on=['Customer','Source','Destination','Transporter','Vehicle Type'],
                         left_index=False, right_index=False, sort=False,
                         suffixes=('_x', '_y'), copy=True, indicator=False,
                         validate=None).drop_duplicates()
        Input_df=Input_1.groupby(['Transporter', 'Source', 'Destination',
                          'Created Date', 'holi','creationmonth',
                          'creationyear', 'creationhour', 'creationweekday',
                          'creationday', 'Indent Type','Total_Indent_Count',
                          'Count_by_route','Vehicle Type' ,
                          'vehicle_count','contract_type','Customer',]).agg({'distance':'mean',
                                                                            'Source Lat':'mean',
                                                                            'Source Long':'mean',
                                                                            'Dest Lat':'mean',
                                                                            'Dest Long':'mean',
                                                                            'Base Freight':'mean',
                                                                             'Gross Weight':'mean',
                                                                             'Actual Freight':'mean',
                                                                             'Transit Time':'mean'}).reset_index()
        ## variable to capture recency
        try:
            Input_df['Created Date']=Input_df['Created Date'].apply(lambda x: DataCorrectImputeUpdate.date_format(x))
        except:
            pass
        date_string = "21 January, 2018"
        refrence_date=datetime.strptime(date_string, "%d %B, %Y")
        Input_df['creation_age']=round(( Input_df['Created Date']-refrence_date ).dt.total_seconds()/(3600*24))
        Input_df=Common_Features.correctdtypes(Input_df)
        df_num_input=Input_df[pd.read_csv(os.path.join(support_file_path,'Placement_Files','Num_Var_PP.csv'))['Num_Var'].values]
        df_obj_input=Input_df[pd.read_csv(os.path.join(support_file_path,'Placement_Files','Obj_Var_PP.csv'))['Obj_Var'].values]
        
        ## Label encoding
        d=pickle.load(open(os.path.join(support_file_path,'Placement_Files','label_coder_PP.sav'),'rb'))
        # Encoding the variable

        df_obj_input_coded=df_obj_input.apply(lambda x: d[x.name].transform(x))

        ##Joinin dataframe to get complete dataframe once again
        Input_df_coded=pd.concat([df_obj_input_coded,df_num_input],1)
        ## Lags Creation
        df_lab=pd.read_csv(os.path.join(processed_datafile_path,'df_lab_PP.csv'))
        df_history=df_lab[['Created Date','Source','Destination','Transporter','Vehicle Type','Target','creationmonth', 'creationyear','creationweekday', 'creationhour', 'creationday']]
#         print(df_history.shape)
        Input_df_coded['Created Date']=Input_df['Created Date']
#         print(Input_df_coded.shape)
        Target_lags=10
        try:
            df_history['Created Date']=df_history['Created Date'].apply(lambda x: DataCorrectImputeUpdate.date_format(x))
        except:
            pass
        lags_Data_all=(df_history[df_history['Created Date']<input_date].sort_values(by='Created Date')['Target'])[-Target_lags:]
        dict_lags={}
        for i,j in enumerate((lags_Data_all)):
            dict_lags['Target_var(t-%d)' % (Target_lags-i)]=[j]

        Target_lags_all=DataFrame(dict_lags)
        Target_lags_all['timestamp']= input_date

        Input_df_coded=pd.merge(Input_df_coded,Target_lags_all, how='inner',
                          on=None, left_on=['Created Date'],
                          right_on=['timestamp'],
                          left_index=False,
                          right_index=False,
                          sort=False,suffixes=('_x', '_y'),
                          copy=True,
                          indicator=False,validate=None).drop_duplicates().drop(['timestamp'],1)
#         print(Input_df_coded.shape)
        # Lags transportwise
        lags_df_trans_wise=DataFrame()

        for t in pd.unique(Input_df_coded['Transporter']):
            dict_lags={}

            df_sample=(df_history[df_history['Transporter']==t]).sort_values(by='Created Date')
            df_sample=df_sample.reset_index().drop(['index'],1)
            ## Auto regressive effect
            lags=10
            for i in ['Source','Destination','Vehicle Type','Target','creationmonth', 'creationyear','creationweekday', 'creationhour', 'creationday']:
                #if len(df_sample)>lags:
                for k,h in enumerate(df_sample[i][-lags:]):
                    dict_lags['Transporter']=[t]
                    dict_lags['{}_var(t-%d)_transporter_wise'.format(i) % (lags-k)]=[h]

            lags_df_trans_wise=lags_df_trans_wise.append(DataFrame(dict_lags))

#         print(Input_df_coded.shape)
        Input_df_coded=pd.merge(Input_df_coded,lags_df_trans_wise, how='left', on=None, left_on=['Transporter'], right_on=['Transporter'],
                  left_index=False, right_index=False, sort=False,
                  suffixes=('_x', '_y'), copy=True, indicator=False,
                  validate=None).drop(['Created Date'],1)
#         print(Input_df_coded.shape)
        Input_df_coded_x=Input_df_coded.copy()
        Input_df_coded_x.columns=list(range(len(Input_df_coded.columns)))
        Output_partial_df=pd.concat([Input_df_coded_x,Input_df],1).dropna()[Input_df.columns]
        Output_partial_df=Output_partial_df.reset_index().drop(['index'],1)
        Input_df_coded=Input_df_coded.dropna()
        columns_all=pd.read_csv(os.path.join(support_file_path,'Placement_Files','variables.csv'))['variables'].values
        best_features=pd.read_csv(os.path.join(support_file_path,'Placement_Files','variables_best_PP.csv'))['Lab_variables'].values
        Input_df_coded=Input_df_coded[columns_all]
        scaler_X=pickle.load(open(os.path.join(support_file_path,'Placement_Files','scaler_X_PP.sav'), 'rb'))
        scaler_Y=pickle.load(open(os.path.join(support_file_path,'Placement_Files','scaler_Y_PP.sav'), 'rb'))
        
        ## Scaling
        predictor_matrix_input=Input_df_coded.values 
        predictor_scaled_matrix=scaler_X.transform(predictor_matrix_input)
        scaled_X_input=pd.DataFrame(predictor_scaled_matrix,columns=columns_all)
        
        predictor_scaled_matrix_best=scaled_X_input[best_features].values
        
        ## Logistic Reg  model= 
        model_LR=pickle.load(open(os.path.join(model_file_path,'place_prob_LR_model.sav'), 'rb'))
        #y_hat_logit = scaler_Y.inverse_transform(model_LR.predict(predictor_scaled_matrix).reshape(-1,1))
        prob_logit=model_LR.predict_proba(predictor_scaled_matrix_best)[:,1]

        Output_partial_df=Output_partial_df[['Customer','Source','Destination','Transporter','Vehicle Type','distance','Gross Weight', 'Actual Freight']]
        Output_partial_df['Probability_Placement']=round(Series(1-prob_logit),2)
        #Output_partial_df['Placement_Rating']=5*Output_partial_df['Probability_Placement']
        #Output_partial_df=Output_partial_df.drop(['Probability_Placement'],1)
        
        
        return Output_partial_df
            
    def delivery_scores(hist_data,input_df_):
    
        now = datetime.now()
        today_date=(pd.to_datetime(now.strftime("%Y-%m-%d %H:%M:%S")))
        input_date=(today_date)
#         input_date=max(hist_data['Created Date'].apply(lambda x: DataCorrectImputeUpdate.date_format(x)))+timedelta(days=1)
#         input_df_['Created Date']=input_date

 
        
        
        input_df_['Created Date']=input_date
        input_df_['Dispatched At']=input_date
        input_df_['Delivery Date']=input_date

        input_df_['Transporter']=input_df_['Transporter'].apply(lambda x: x.strip().capitalize())


        ## Special days
        Input_df=Common_Features.hdays(input_df_,'Created Date')
        Input_df=Common_Features.hdays(input_df_,'Dispatched At')
        Input_df=Common_Features.hdays(input_df_,'Delivery Date')


        ## creating calender days
        Input_df=Common_Features.calender_days(input_df_,'Created Date','creation')
        Input_df=Common_Features.calender_days(input_df_,'Dispatched At','Dispatched')
        Input_df=Common_Features.calender_days(input_df_,'Delivery Date','Delivery')



        hist_data_ml_dlv=pd.read_csv(os.path.join(processed_datafile_path,'hist_data_ml_dlv.csv'))
        df_help=hist_data_ml_dlv.drop(['holi',



                                        'Arrived At',
                                        'Dispatched At',

                                        'Delivery Date',
                                        'Created Date',
                                        'Indent ID',



                                        'Target',
                                        'creationmonth',
                                        'creationyear', 
                                        'creationhour',
                                        'creationweekday',
                                        'creationday',
                                        'Dispatchedmonth',
                                        'Dispatchedyear',
                                        'Dispatchedhour',
                                        'Dispatchedweekday',
                                        'Dispatchedday',
                                        'Deliverymonth',
                                        'Deliveryyear',
                                        'Deliveryhour',
                                        'Deliveryweekday',
                                        'Deliveryday'],1)




        Input_1=pd.merge(Input_df,df_help, how='inner', on=None,
                                 left_on=['Customer','Source','Destination','Transporter','Vehicle Type'],
                                 right_on=['Customer','Source','Destination','Transporter','Vehicle Type'],
                                 left_index=False, right_index=False, sort=False,
                                 suffixes=('_x', '_y'), copy=True, indicator=False,
                                 validate=None).drop_duplicates()
        
        Input_df=Input_1.groupby(['Transporter', 'Source', 'Destination',
                                  'Created Date', 'holi','creationmonth',
                                  'creationyear','Dispatchedmonth', 'Dispatchedyear',
                                  'Dispatchedhour','Dispatchedweekday', 'Dispatchedday',
                                  'Deliverymonth', 'Deliveryyear','Deliveryhour', 'Deliveryweekday',
                                  'Deliveryday', 'creationhour', 'creationweekday',
                                  'creationday','Indent Type',
                                  'Total_Indent_Count','Count_by_route','Vehicle Type',
                                  'vehicle_count','contract_type','Customer',
                                 'Placement_Status',]).agg({'distance':'mean',
                                                           'Source Lat':'mean',
                                                           'Source Long':'mean',
                                                           'Dest Lat':'mean',
                                                           'Dest Long':'mean',
                                                           'Base Freight':'mean',
                                                           'Gross Weight':'mean',
                                                           'Actual Freight':'mean',
                                                           'Transit Time':'mean',
                                                            'time_to_delv':'mean', 

                                                            'sla_delay_charges':'mean'}).reset_index()










        ## variable to capture recency
        try:
            Input_df['Created Date']=Input_df['Created Date'].apply(lambda x: DataCorrectImputeUpdate.date_format(x))
        except:
            pass
        date_string = "21 January, 2018"
        refrence_date=datetime.strptime(date_string, "%d %B, %Y")
        Input_df['creation_age']=round(( Input_df['Created Date']-refrence_date ).dt.total_seconds()/(3600*24))

        Input_df=Common_Features.correctdtypes(Input_df)
        df_num_input=Input_df[pd.read_csv(os.path.join(support_file_path,'Delivery_Files','Num_Var_Dlv.csv'))['Num_Var'].values]
        df_obj_input=Input_df[pd.read_csv(os.path.join(support_file_path,'Delivery_Files','Obj_Var_Dlv.csv'))['Obj_Var'].values]
        ## Label encoding
        ## Label encoding
        d=pickle.load(open(os.path.join(support_file_path,'Delivery_Files','label_coder_Dlv.sav'),'rb'))
        # Encoding the variable

        # Encoding the variable

        df_obj_input_coded=df_obj_input.apply(lambda x: d[x.name].transform(x))

        ##Joinin dataframe to get complete dataframe once again
        Input_df_coded=pd.concat([df_obj_input_coded,df_num_input],1)

        ## Lags Creation
        df_lab=pd.read_csv(os.path.join(processed_datafile_path,'df_lab_Dlv.csv'))
        df_history=df_lab[['Created Date','Source','Destination','Transporter','sla_delay_charges','Vehicle Type','Target','creationmonth', 'creationyear','creationweekday', 'creationhour', 'creationday','Placement_Status','Deliverymonth', 'Deliveryyear','Deliveryweekday', 'Deliveryhour', 'Deliveryday','Dispatchedmonth','Dispatchedyear','Dispatchedweekday', 'Dispatchedhour', 'Dispatchedday']]
#         print(df_history.shape)
        Input_df_coded['Created Date']=Input_df['Created Date']
#         print(Input_df_coded.shape)
        Target_lags=10
        try:
            df_history['Created Date']=df_history['Created Date'].apply(lambda x: DataCorrectImputeUpdate.date_format(x))
        except:
            pass
        lags_Data_all=(df_history[df_history['Created Date']<input_date].sort_values(by='Created Date')['Target'])[-Target_lags:]
        dict_lags={}
        for i,j in enumerate((lags_Data_all)):
            dict_lags['Target_var(t-%d)' % (Target_lags-i)]=[j]

        Target_lags_all=DataFrame(dict_lags)
        Target_lags_all['timestamp']= input_date

        Input_df_coded=pd.merge(Input_df_coded,Target_lags_all, how='inner',
                          on=None, left_on=['Created Date'],
                          right_on=['timestamp'],
                          left_index=False,
                          right_index=False,
                          sort=False,suffixes=('_x', '_y'),
                          copy=True,
                          indicator=False,validate=None).drop_duplicates().drop(['timestamp'],1)
#         print(Input_df_coded.shape)
        # Lags transportwise
        lags_df_trans_wise=DataFrame()

        for t in pd.unique(Input_df_coded['Transporter']):
            dict_lags={}

            df_sample=(df_history[df_history['Transporter']==t]).sort_values(by='Created Date')
            df_sample=df_sample.reset_index().drop(['index'],1)
            ## Auto regressive effect
            lags=10
            for i in ['Source','Destination','Vehicle Type','sla_delay_charges','Target','creationmonth', 'creationyear','creationweekday', 'creationhour', 'creationday','Placement_Status','Deliverymonth', 'Deliveryyear','Deliveryweekday', 'Deliveryhour', 'Deliveryday','Dispatchedmonth','Dispatchedyear','Dispatchedweekday', 'Dispatchedhour', 'Dispatchedday']:
                #if len(df_sample)>lags:
                for k,h in enumerate(df_sample[i][-lags:]):
                    dict_lags['Transporter']=[t]
                    dict_lags['{}_var(t-%d)_transporter_wise'.format(i) % (lags-k)]=[h]

            lags_df_trans_wise=lags_df_trans_wise.append(DataFrame(dict_lags))

#         print(Input_df_coded.shape)
        
        Input_df_coded=pd.merge(Input_df_coded,lags_df_trans_wise, how='left', on=None, left_on=['Transporter'], right_on=['Transporter'],
                          left_index=False, right_index=False, sort=False,
                          suffixes=('_x', '_y'), copy=True, indicator=False,
                          validate=None).drop(['Created Date'],1)
#         print(Input_df_coded.shape)
        Input_df_coded_x=Input_df_coded.copy()
        Input_df_coded_x.columns=list(range(len(Input_df_coded.columns)))
        Output_partial_df=pd.concat([Input_df_coded_x,Input_df],1).dropna()[Input_df.columns]
        Output_partial_df=Output_partial_df.reset_index().drop(['index'],1)
        Input_df_coded=Input_df_coded.dropna()
        columns_all=pd.read_csv(os.path.join(support_file_path,'Delivery_Files','variables.csv'))['variables'].values
        best_features=pd.read_csv(os.path.join(support_file_path,'Delivery_Files','variables_best_dlv.csv'))['Lab_variables'].values
        
        Input_df_coded=Input_df_coded[columns_all]
        scaler_X=pickle.load(open(os.path.join(support_file_path,'Delivery_Files','scaler_X_Dlv.sav'), 'rb'))
        scaler_Y=pickle.load(open(os.path.join(support_file_path,'Delivery_Files','scaler_Y_Dlv.sav'), 'rb'))
        ## Scaling

        predictor_matrix_input=Input_df_coded.values

        # scaler_X 
        # scaler_Y 
        predictor_scaled_matrix=scaler_X.transform(predictor_matrix_input)


        scaled_X_input=pd.DataFrame(predictor_scaled_matrix,columns=columns_all)

        predictor_scaled_matrix_best=scaled_X_input[best_features].values
        
        ## Logistic Reg
        model_LR=pickle.load(open(os.path.join(model_file_path,'dlv_prob_LR_model.sav'), 'rb'))
        #y_hat_logit = scaler_Y.inverse_transform(model_LR.predict(predictor_scaled_matrix).reshape(-1,1))
        prob_logit=model_LR.predict_proba(predictor_scaled_matrix_best)[:,1]
        Output_partial_df=Output_partial_df[['Customer','Source','Destination','Transporter','Vehicle Type','distance','Gross Weight', 'Actual Freight']]
        Output_partial_df['Probability_On-Time_Delivery']=round(Series(prob_logit),2)
        #Output_partial_df['On-Time_Delivery_Rating']=5*Output_partial_df['Probability_On-Time_Delivery']
        
        return Output_partial_df
 
    
    def dfs_scores(hist_data,input_df_):
        now = datetime.now()
        today_date=(pd.to_datetime(now.strftime("%Y-%m-%d %H:%M:%S")))
        input_date=(today_date)
#         input_date=max(hist_data['Created Date'].apply(lambda x: DataCorrectImputeUpdate.date_format(x)))+timedelta(days=1)
        input_df_['Created Date']=input_date

        
        input_df_['Created Date']=input_date
        input_df_['Dispatched At']=input_date
        input_df_['Delivery Date']=input_date

        input_df_['Transporter']=input_df_['Transporter'].apply(lambda x: x.strip().capitalize())


        ## Special days
        Input_df=Common_Features.hdays(input_df_,'Created Date')
        Input_df=Common_Features.hdays(input_df_,'Dispatched At')
        Input_df=Common_Features.hdays(input_df_,'Delivery Date')


        ## creating calender days
        Input_df=Common_Features.calender_days(input_df_,'Created Date','creation')
        Input_df=Common_Features.calender_days(input_df_,'Dispatched At','Dispatched')
        Input_df=Common_Features.calender_days(input_df_,'Delivery Date','Delivery')

        hist_data_ml_cost=pd.read_csv(os.path.join(processed_datafile_path,'hist_data_ml_cost.csv'))
        df_help=hist_data_ml_cost.drop(['holi',



                                        'Arrived At',
                                        'Dispatched At',

                                        'Delivery Date',
                                        'Created Date',
                                        'Indent ID',



                                        'Target',
                                        'creationmonth',
                                        'creationyear', 
                                        'creationhour',
                                        'creationweekday',
                                        'creationday',
                                        'Dispatchedmonth',
                                        'Dispatchedyear',
                                        'Dispatchedhour',
                                        'Dispatchedweekday',
                                        'Dispatchedday',
                                        'Deliverymonth',
                                        'Deliveryyear',
                                        'Deliveryhour',
                                        'Deliveryweekday',
                                        'Deliveryday'],1)

        Input_1=pd.merge(Input_df,df_help, how='inner', on=None,
                                 left_on=['Customer','Source','Destination','Transporter','Vehicle Type'],
                                 right_on=['Customer','Source','Destination','Transporter','Vehicle Type'],
                                 left_index=False, right_index=False, sort=False,
                                 suffixes=('_x', '_y'), copy=True, indicator=False,
                                 validate=None).drop_duplicates()

        Input_df=Input_1.groupby(['Transporter', 'Source', 'Destination',
                                  'Created Date', 'holi','creationmonth',
                                  'creationyear','Dispatchedmonth', 'Dispatchedyear',
                                  'Dispatchedhour','Dispatchedweekday', 'Dispatchedday',
                                  'Deliverymonth', 'Deliveryyear','Deliveryhour', 'Deliveryweekday',
                                  'Deliveryday', 'creationhour', 'creationweekday',
                                  'creationday','Indent Type',
                                  'Total_Indent_Count','Count_by_route','Vehicle Type',
                                  'vehicle_count','contract_type','Customer','OnTimeStatus',
                                 'Placement_Status',]).agg({'distance':'mean',
                                                           'Source Lat':'mean',
                                                           'Source Long':'mean',
                                                           'Dest Lat':'mean',
                                                           'Dest Long':'mean',
                                                           'Base Freight':'mean',
                                                           'Gross Weight':'mean',
                                                           'Actual Freight':'mean',
                                                           'Transit Time':'mean',
                                                            'damage_charges':'mean',
                                                            'time_to_delv':'mean', 
                                                            'shortage_charges':'mean',
                                                            'sla_delay_charges':'mean',
                                                            'carton_damage_charges':'mean'}).reset_index()










        ## variable to capture recency
        try:
            Input_df['Created Date']=Input_df['Created Date'].apply(lambda x: DataCorrectImputeUpdate.date_format(x))
        except:
            pass
        date_string = "21 January, 2018"
        refrence_date=datetime.strptime(date_string, "%d %B, %Y")
        Input_df['creation_age']=round(( Input_df['Created Date']-refrence_date ).dt.total_seconds()/(3600*24))

        Input_df=Common_Features.correctdtypes(Input_df)
        df_num_input=Input_df[pd.read_csv(os.path.join(support_file_path,'DFS_Files','Num_Var_DFS.csv'))['Num_Var'].values]
        df_obj_input=Input_df[pd.read_csv(os.path.join(support_file_path,'DFS_Files','Obj_Var_DFS.csv'))['Obj_Var'].values]

        ## Label encoding
        d=pickle.load(open(os.path.join(support_file_path,'DFS_Files','label_coder_DFS.sav'),'rb'))
        # Encoding the variable

        df_obj_input_coded=df_obj_input.apply(lambda x: d[x.name].transform(x))

        ##Joinin dataframe to get complete dataframe once again
        Input_df_coded=pd.concat([df_obj_input_coded,df_num_input],1)

        ## Lags Creation
        df_lab=pd.read_csv(os.path.join(processed_datafile_path,'df_lab_DFS.csv'))

        df_history=df_lab[['Created Date','Source','Destination','sla_delay_charges','Transporter','Vehicle Type','Target','creationmonth', 'creationyear','creationweekday', 'creationhour', 'creationday','Placement_Status','OnTimeStatus','Deliverymonth', 'Deliveryyear','Deliveryweekday', 'Deliveryhour', 'Deliveryday','Dispatchedmonth','Dispatchedyear','Dispatchedweekday', 'Dispatchedhour', 'Dispatchedday']]
#         print(df_history.shape)
        Input_df_coded['Created Date']=Input_df['Created Date']
#         print(Input_df_coded.shape)
        Target_lags=10
        try:
            df_history['Created Date']=df_history['Created Date'].apply(lambda x: DataCorrectImputeUpdate.date_format(x))
        except:
            pass
        lags_Data_all=(df_history[df_history['Created Date']<input_date].sort_values(by='Created Date')['Target'])[-Target_lags:]
        dict_lags={}
        for i,j in enumerate((lags_Data_all)):
            dict_lags['Target_var(t-%d)' % (Target_lags-i)]=[j]

        Target_lags_all=DataFrame(dict_lags)
        Target_lags_all['timestamp']= input_date

        Input_df_coded=pd.merge(Input_df_coded,Target_lags_all, how='inner',
                          on=None, left_on=['Created Date'],
                          right_on=['timestamp'],
                          left_index=False,
                          right_index=False,
                          sort=False,suffixes=('_x', '_y'),
                          copy=True,
                          indicator=False,validate=None).drop_duplicates().drop(['timestamp'],1)
#         print(Input_df_coded.shape)
        # Lags transportwise
        lags_df_trans_wise=DataFrame()

        for t in pd.unique(Input_df_coded['Transporter']):
            dict_lags={}

            df_sample=(df_history[df_history['Transporter']==t]).sort_values(by='Created Date')
            df_sample=df_sample.reset_index().drop(['index'],1)
            ## Auto regressive effect
            lags=10
            for i in ['Source','Destination','Vehicle Type','Target','sla_delay_charges','creationmonth', 'creationyear','creationweekday', 'creationhour', 'creationday','Placement_Status','OnTimeStatus','Deliverymonth', 'Deliveryyear','Deliveryweekday', 'Deliveryhour', 'Deliveryday','Dispatchedmonth','Dispatchedyear','Dispatchedweekday', 'Dispatchedhour', 'Dispatchedday']:
                #if len(df_sample)>lags:
                for k,h in enumerate(df_sample[i][-lags:]):
                    dict_lags['Transporter']=[t]
                    dict_lags['{}_var(t-%d)_transporter_wise'.format(i) % (lags-k)]=[h]

            lags_df_trans_wise=lags_df_trans_wise.append(DataFrame(dict_lags))

#         print(Input_df_coded.shape)
        Input_df_coded=pd.merge(Input_df_coded,lags_df_trans_wise, how='left', on=None, left_on=['Transporter'], right_on=['Transporter'],
                          left_index=False, right_index=False, sort=False,
                          suffixes=('_x', '_y'), copy=True, indicator=False,
                          validate=None).drop(['Created Date'],1)
#         print(Input_df_coded.shape)
        Input_df_coded_x=Input_df_coded.copy()
        Input_df_coded_x.columns=list(range(len(Input_df_coded.columns)))
        Output_partial_df=pd.concat([Input_df_coded_x,Input_df],1).dropna()[Input_df.columns]
        Output_partial_df=Output_partial_df.reset_index().drop(['index'],1)
        Input_df_coded=Input_df_coded.dropna()
        columns_all=pd.read_csv(os.path.join(support_file_path,'DFS_Files','variables_classif.csv'))['variables'].values
        best_features=pd.read_csv(os.path.join(support_file_path,'DFS_Files','variables_best_DFS.csv'))['Lab_variables'].values
        Input_df_coded=Input_df_coded[columns_all]
        scaler_X=pickle.load(open(os.path.join(support_file_path,'DFS_Files','scaler_X_class.sav'), 'rb'))
        scaler_Y=pickle.load(open(os.path.join(support_file_path,'DFS_Files','scaler_Y_class.sav'), 'rb'))
        
        ## Scaling
        predictor_matrix_input=Input_df_coded.values
        predictor_scaled_matrix=scaler_X.transform(predictor_matrix_input)
        scaled_X_input=pd.DataFrame(predictor_scaled_matrix,columns=columns_all)
        predictor_scaled_matrix_best=scaled_X_input[best_features].values
        ## Logistic Reg
        model_LR=pickle.load(open(os.path.join(model_file_path,'DFS_prob_LR_model.sav'), 'rb'))
        y_hat_logit = scaler_Y.inverse_transform(model_LR.predict(predictor_scaled_matrix_best).reshape(-1,1))
        prob_logit=model_LR.predict_proba(predictor_scaled_matrix_best)[:,1]

        df=Input_df_coded.copy()
        df['damage_status']=y_hat_logit
        
        df_reg=df[df.damage_status==1]
        df_reg=df_reg.drop(['damage_status'],1)[pd.read_csv(os.path.join(support_file_path,'DFS_Files','DFSRegression_Var.csv'))['DFSRegression_Var'].values]
        scaler_X_reg=pickle.load(open(os.path.join(support_file_path,'DFS_Files','scaler_X_reg.sav'), 'rb'))
        scaler_Y_reg=pickle.load(open(os.path.join(support_file_path,'DFS_Files','scaler_Y_reg.sav'), 'rb'))
        
        pm=scaler_X_reg.transform(df_reg.values)
        ## ridge Reg
        model_ridge=pickle.load(open(os.path.join(model_file_path,'DFS_ridge_model.sav'), 'rb'))

        ##predict
        y_hat_ridge=scaler_Y_reg.inverse_transform(model_ridge.predict(pm).reshape(-1,1))
        y_hat_ridge[y_hat_ridge<0]=0
        Output_partial_df=Output_partial_df[['Customer','Source','Destination','Transporter','Vehicle Type','distance','Gross Weight', 'Actual Freight']]
        Output_partial_df['DamageFree_Shipment_Probability']=round(Series(1-prob_logit),2)
        Output_partial_df['damage_status']=y_hat_logit
        Output_partial_df_no_damage=Output_partial_df[Output_partial_df['damage_status']==0]
        Output_partial_df_damage=Output_partial_df[Output_partial_df['damage_status']==1]
        Output_partial_df_damage['Damage_to_Freight_ratio']=y_hat_ridge
        out_df=Output_partial_df_damage.append(Output_partial_df_no_damage)
        out_df=out_df.sample(frac=1)
        out_df['Damage_to_Freight_ratio']=out_df.apply(lambda x: x.damage_status if x.damage_status==0 else x.Damage_to_Freight_ratio,axis=1)
        out_df['Expected_Damage_to_Freight_ratio']=out_df.apply(lambda x: (1-x.DamageFree_Shipment_Probability)*x.Damage_to_Freight_ratio,axis=1)
        out_df=out_df.drop(['damage_status'],1)
        
        
        ## Scale damage percentage

        out_df=out_df.groupby(['Source','Destination']).apply(TransportersScore.dfs_rating_)

        output_final=out_df[['Customer',
                             'Source',
                             'Destination',
                             'Transporter',
                             'Vehicle Type',
                             'distance',
                             'Gross Weight',
                             'Actual Freight',
                             #'DamageFree_Shipment_Rating',
                             'Relative_Expected_Damage_to_Freight_ratio']]




        return output_final

    def dfs_rating_(sample):

        scaler_rating= MinMaxScaler(copy=True, feature_range=(0, 1))
        sample['Relative_Expected_Damage_to_Freight_ratio']=scaler_rating.fit_transform((sample.Expected_Damage_to_Freight_ratio).values.reshape(-1,1))
        sample['Relative_Expected_Damage_to_Freight_ratio']=sample['Relative_Expected_Damage_to_Freight_ratio'].apply(lambda x:round(x,2))
        return sample


pp_scores=TransportersScore.pp_scores(hist_data,input_df_)

delivery_scores=TransportersScore.delivery_scores(hist_data,input_df_)

dfs_scores=TransportersScore.dfs_scores(hist_data,input_df_)
dfs_scores['DamageFree_Shipment_Score']=dfs_scores['Relative_Expected_Damage_to_Freight_ratio'].apply(lambda x:(1-x))
dfs_scores=dfs_scores.drop(['Relative_Expected_Damage_to_Freight_ratio'],1)


hist_data_ml_PP=pd.read_csv(os.path.join(processed_datafile_path,'hist_data_ml_PP.csv'))
hist_data_ml_dlv=pd.read_csv(os.path.join(processed_datafile_path,'hist_data_ml_dlv.csv'))
hist_data_ml_cost=pd.read_csv(os.path.join(processed_datafile_path,'hist_data_ml_cost.csv'))

 

## Create target Variable for static Ratings

#Placement
hist_data_static_PP=hist_data.copy()
hist_data_static_PP= hist_data_static_PP[hist_data_static_PP['Transporter'] != 'Dummy']
hist_data_static_PP['Arrival Breached At']=hist_data_static_PP['Arrival Breached At'].fillna(0)
hist_data_static_PP['Arrival Breached At']=hist_data_static_PP['Arrival Breached At'].apply(lambda x: 0 if x =='NaT' else x)

hist_data_static_PP['Target']=hist_data_static_PP['Arrival Breached At'].apply(lambda x : 1 if x!=0 else x)
hist_data_static_PP=hist_data_static_PP.drop(['Arrival Breached At'],1)
hist_data_static_PP=hist_data_static_PP.dropna(subset=['Created Date','Target'])
 
## Delivery
hist_data_static_dlv=hist_data.copy()
hist_data_static_dlv= hist_data_static_dlv[hist_data_static_dlv['Transporter'] != 'Dummy']
hist_data_static_dlv=hist_data_static_dlv[hist_data_static_dlv['Delivery Date']!='NaT'] 
hist_data_static_dlv=hist_data_static_dlv[hist_data_static_dlv['Dispatched At']!='NaT'] 
hist_data_static_dlv=hist_data_static_dlv.dropna(subset=['Created Date','Arrived At','Dispatched At','Delivery Date'])

for j in ['Created Date','Arrived At','Dispatched At','Delivery Date'] :
    try:
        hist_data_static_dlv[j]=hist_data_static_dlv[j].apply(lambda x:DataCorrectImputeUpdate.date_format(x))
    except:
        pass

hist_data_static_dlv['time_to_delv']=(hist_data_static_dlv['Delivery Date']-hist_data_static_dlv['Dispatched At']).dt.total_seconds()/3600
hist_data_static_dlv[hist_data_static_dlv.time_to_delv>0]

df_contract=hist_data_static_dlv[hist_data_static_dlv['Indent Type']=='Contract']
df_contract=df_contract[df_contract['Transit Time']!=0]
df_contract['Transit Time']=df_contract['Transit Time']*24

df_open=hist_data_static_dlv[hist_data_static_dlv['Indent Type']=='Open']

## 'On time delivery Status'

df_contract['Target']=df_contract[['Transit Time','time_to_delv']].apply(lambda x: 0 if x.time_to_delv>x['Transit Time'] else 1,axis=1)

df_open['Target']=df_open['sla_delay_charges'].apply(lambda x: 0 if x>0 else 1)

hist_data_static_dlv=df_contract.append(df_open)
hist_data_static_dlv=hist_data_static_dlv.dropna(subset=['Created Date','Target'])
 

## Target Variable Damages
hist_data_static_dfs=hist_data.copy()
 

hist_data_static_dfs= hist_data_static_dfs[hist_data_static_dfs['Transporter'] != 'Dummy']
hist_data_static_dfs=hist_data_static_dfs[hist_data_static_dfs['Actual Freight']>0]
hist_data_static_dfs['Target']=((0.6*hist_data_static_dfs['shortage_charges']) + (0.3*hist_data_static_dfs['damage_charges']) + (0.1*hist_data_static_dfs['carton_damage_charges']))/hist_data_static_dfs['Actual Freight']
hist_data_static_dfs=hist_data_static_dfs.dropna(subset=['Created Date','Target'])
 


## Calculate Static Probabilities

def ewm_pp(sample):
    try:
        sample['Created Date']=sample['Created Date'].apply(lambda x:DataCorrectImputeUpdate.date_format(x))
    except:
        pass
    sample=sample.sort_values(by=['Created Date'], ascending=True)
    sample['number_event']=list(range(1,len(sample)+1))
    sample['Cumsum_satus']=sample['Target'].cumsum()
    sample['Probability']=sample['Cumsum_satus']/sample['number_event']
    sample['Probability_Decayed']=sample['Probability'].ewm(alpha=0.15).mean()
    sample['Placement_Rating']=sample['Probability_Decayed'] * 5
    sample['Placement_Rating']=sample['Placement_Rating'].apply(lambda x: 5-x)
    sample['Placement_Rating']=sample['Placement_Rating'].apply(lambda x:round(x,2))
    sample=sample[['Placement_Rating']].iloc[-1:]
    
    return sample

def ewm_dlv(sample):
    
    try:
        sample['Created Date']=sample['Created Date'].apply(lambda x:DataCorrectImputeUpdate.date_format(x))
    except:
        pass
    sample=sample.sort_values(by=['Created Date'], ascending=True)
    sample['number_event']=list(range(1,len(sample)+1))
    sample['Cumsum_satus']=sample['Target'].cumsum()
    sample['Probability']=sample['Cumsum_satus']/sample['number_event']
    sample['Probability']= sample['Probability'].apply(lambda x:round(x,2))
    sample['Probability_Decayed']=sample['Probability'].ewm(alpha=0.15).mean()
    sample['Delivery_Rating']=sample['Probability_Decayed'] * 5
    sample['Delivery_Rating']=sample['Delivery_Rating'].apply(lambda x:round(x,2))
    sample=sample[['Delivery_Rating']].iloc[-1:]
    return sample


def ewm_dfs(sample):
    try:
        sample['Created Date']=sample['Created Date'].apply(lambda x:DataCorrectImputeUpdate.date_format(x))
    except:
        pass
    sample=sample.sort_values(by=['Created Date'], ascending=True)
 
    sample['damage_status']=sample['Target'].apply(lambda x: 1 if x >0 else 0)
    sample['number_event']=list(range(1,len(sample)+1))
    sample['Cumsum_satus']=sample['Target'].cumsum()
    sample['Probability']=sample['Cumsum_satus']/sample['number_event']
     
    sample['Probability_Decayed']=sample['Probability'].ewm(alpha=0.15).mean()
 
    sample['Expected_Damage']=sample['Probability_Decayed']*sample['Target']
    
    sample=sample[['Expected_Damage']].iloc[-1:]
    
    return sample

def dfs_rating(sample):
    scaler_rating= MinMaxScaler(copy=True, feature_range=(0, 1))
    sample['Expected_Damage']=scaler_rating.fit_transform((sample.Expected_Damage).values.reshape(-1,1))
    sample['DamageFree_Shipment_Rating']=sample['Expected_Damage'].apply(lambda x: 5-(5*x))
    sample=sample.drop(['Expected_Damage'],1)
    return sample
   

static_pp=hist_data_static_PP[['Created Date',
                          'Customer', 
                          'Source',
                          'Destination',
                          'Transporter',
                          'Vehicle Type','Target']].groupby(['Customer',  
                  'Source',
                  'Destination',
                  'Transporter',
                  'Vehicle Type']).apply(ewm_pp)

static_dlv=hist_data_static_dlv[['Created Date',
                          'Customer', 
                          'Source',
                          'Destination',
                          'Transporter',
                          'Vehicle Type','Target']].groupby(['Customer',  
                  'Source',
                  'Destination',
                  'Transporter',
                  'Vehicle Type']).apply(ewm_dlv)

static_dfs=hist_data_static_dfs[['Created Date',
                          'Customer', 
                          'Source',
                          'Destination',
                          'Transporter',
                          'Vehicle Type','Target']].groupby(['Customer',  
                  'Source',
                  'Destination',
                  'Transporter',
                  'Vehicle Type']).apply(ewm_dfs)


# In[57]:


static_pp=static_pp.reset_index().drop(['level_5'],1)
 


# In[58]:


static_dlv=static_dlv.reset_index().drop(['level_5'],1)
# static_dlv


# In[59]:


static_dfs=static_dfs.reset_index().drop(['level_5'],1)
static_dfs=static_dfs.groupby(['Source','Destination']).apply(dfs_rating)
# static_dfs


# In[60]:


static_pp_dlv=pd.merge(static_pp,static_dlv,how='left',on=['Customer','Source','Destination','Transporter','Vehicle Type'])
static_pp_dlv_dfs=pd.merge(static_pp_dlv,static_dfs,how='left',on=['Customer','Source','Destination','Transporter','Vehicle Type'])


# In[61]:


static_pp_dlv_dfs=pd.merge(static_pp_dlv_dfs,indent_df,how='inner',on=['Customer','Source', 'Destination', 'Transporter','Vehicle Type'])


# In[62]:


pp_scores.to_csv(os.path.join(model_out_path,'Placement_output','Placement_Rating.csv'),index=False)
delivery_scores.to_csv(os.path.join(model_out_path,'Delivery_output','Delivery_Rating.csv'),index=False)
dfs_scores.to_csv(os.path.join(model_out_path,'DFS_output','DFS_Rating.csv'),index=False)


# In[70]:


def cost_per_kg(df):


    df['Cost/Kg'] = 0
    df['Cost/Kg'] = (df['Base Freight'] * 1000).where(df['contract_type'] == "PER_TON", df['Cost/Kg'])
    df['Cost/Kg'] = df['Base Freight'].where(df['contract_type'] == "PER_KG", df['Cost/Kg'])
    df['Cost/Kg'] = (df['Base Freight'] / df['Gross Weight']).where(df['contract_type'] == "PER_TRIP", df['Cost/Kg'])


    df_cost_kg=df[['Customer','Source','Destination','Transporter','Vehicle Type','contract_type','Cost/Kg']]
    df_cost_kg=df_cost_kg.dropna(subset=['Transporter'])
    df_cost_kg['Transporter']=df_cost_kg['Transporter'].apply(lambda x: x.strip().capitalize())

    return df_cost_kg


# In[71]:


df_cost_per_kg=cost_per_kg(hist_data).drop_duplicates(subset=['Customer','Source','Destination','Transporter','Vehicle Type'])


# In[72]:


PR_agg=pp_scores.groupby(['Customer', 'Source', 'Destination', 'Transporter', 'Vehicle Type']).agg({'distance':'mean',
                                                                                      'Probability_Placement':'mean'}).reset_index()

DR_agg=delivery_scores.groupby(['Customer', 'Source', 'Destination', 'Transporter', 'Vehicle Type']).agg({'distance':'mean',
                                                                                      'Probability_On-Time_Delivery':'mean'}).reset_index()

Dmg_R_agg=dfs_scores.groupby(['Customer', 'Source', 'Destination', 'Transporter', 'Vehicle Type']).agg({'distance':'mean',
                                                                                                   'DamageFree_Shipment_Score':'mean'}).reset_index()
            


# In[73]:


PR_DR=pd.merge(PR_agg,DR_agg,how='left',on=['Customer', 'Source', 'Destination', 'Transporter', 'Vehicle Type','distance'])

PR_DR_DmgR=pd.merge(PR_DR,Dmg_R_agg,how='left',on=['Customer', 'Source', 'Destination', 'Transporter', 'Vehicle Type','distance'])

PR_DR_DmgR=pd.merge(PR_DR_DmgR,df_cost_per_kg,how='inner',on=['Customer', 'Source', 'Destination', 'Transporter', 'Vehicle Type'])


 

## Merge Ratings
All_rating=pd.merge(static_pp_dlv_dfs,PR_DR_DmgR,how='left',on=['Customer', 'Source', 'Destination', 'Transporter', 'Vehicle Type'])
All_rating=All_rating[['Customer','Source','Destination','Transporter','Vehicle Type','distance','contract_type','Cost/Kg','Placement_Rating', 'Delivery_Rating', 'DamageFree_Shipment_Rating','Indent_Count_lanewise','Total_Indent_Count',
                        'Probability_Placement','Probability_On-Time_Delivery','DamageFree_Shipment_Score']]
   
# All_rating


 

All_rating.columns=['Customer', 'Source', 'Destination', 'Transporter', 'Vehicle Type',
       'distance', 'contract_type', 'Cost/Kg', 'Placement_Rating',
       'Delivery_Rating', 'DamageFree_Shipment_Rating',
       'Indent_Count_LaneVT', 'Total_Indent_Count', 'Placement_Probability',
       'On-Time_Delivery_Probability', 'DamageFree_Shipment_Score']

 

Rating=All_rating[['Customer',
                   'Source',
                   'Destination',
                   'Transporter',
                   'Vehicle Type',
                   #'distance',
                   'contract_type',
                   'Cost/Kg',
                   'Indent_Count_LaneVT',
                   'Total_Indent_Count',
                   'Placement_Rating',
                   'Delivery_Rating',
                   'DamageFree_Shipment_Rating',
                   'Placement_Probability',
                   'On-Time_Delivery_Probability',
                   'DamageFree_Shipment_Score']].drop_duplicates(subset=['Customer',
            'Source',
            'Destination',
            'Transporter',
            'Vehicle Type'])


# In[80]:


Rating['Cost/Kg']=Rating['Cost/Kg'].apply(lambda x : round(x,2))
Rating['Indent_Count_LaneVT']=Rating['Indent_Count_LaneVT'].apply(lambda x : round(x,2))
Rating['Placement_Rating']=Rating['Placement_Rating'].apply(lambda x : round(x,2))
Rating['Delivery_Rating']=Rating['Delivery_Rating'].apply(lambda x : round(x,2))
Rating['DamageFree_Shipment_Rating']=Rating['DamageFree_Shipment_Rating'].apply(lambda x : round(x,2))

Rating['Placement_Probability']=Rating['Placement_Probability'].apply(lambda x : round(x,2))
Rating['On-Time_Delivery_Probability']=Rating['On-Time_Delivery_Probability'].apply(lambda x : round(x,2))
Rating['DamageFree_Shipment_Score']=Rating['DamageFree_Shipment_Score'].apply(lambda x : round(x,2))


 

Rating.insert(9,column='Confidence_Score',value=round(np.log( Rating['Indent_Count_LaneVT'])/np.log(Rating['Total_Indent_Count']),2))


# In[84]:


Rating['Confidence_Score']=Rating['Confidence_Score'].fillna(0)


# In[85]:


def weight_agg(sample_df):
     
    sample_df['Placement_Rating']=sample_df['Confidence_Score'] * sample_df['Placement_Rating']
    sample_df['Placement_Rating']=sample_df['Placement_Rating']/sum(sample_df.dropna(subset=['Placement_Rating'])['Confidence_Score'])
    
    sample_df['Delivery_Rating']=sample_df['Confidence_Score'] * sample_df['Delivery_Rating']
    sample_df['Delivery_Rating']=sample_df['Delivery_Rating']/sum(sample_df.dropna(subset=['Delivery_Rating'])['Confidence_Score'])
    
    sample_df['DamageFree_Shipment_Rating']=sample_df['Confidence_Score'] * sample_df['DamageFree_Shipment_Rating']
    sample_df['DamageFree_Shipment_Rating']=sample_df['DamageFree_Shipment_Rating']/sum(sample_df.dropna(subset=['DamageFree_Shipment_Rating'])['Confidence_Score'])
 

 
    sample_df['Placement_Probability']=sample_df['Confidence_Score'] * sample_df['Placement_Probability']
    sample_df['Placement_Probability']=sample_df['Placement_Probability']/sum(sample_df.dropna(subset=['Placement_Probability'])['Confidence_Score'])
    
    sample_df['On-Time_Delivery_Probability']=sample_df['Confidence_Score'] * sample_df['On-Time_Delivery_Probability']
    sample_df['On-Time_Delivery_Probability']=sample_df['On-Time_Delivery_Probability']/sum(sample_df.dropna(subset=['On-Time_Delivery_Probability'])['Confidence_Score'])
    
    sample_df['DamageFree_Shipment_Score']=sample_df['Confidence_Score'] * sample_df['DamageFree_Shipment_Score']
    sample_df['DamageFree_Shipment_Score']=sample_df['DamageFree_Shipment_Score']/sum(sample_df.dropna(subset=['DamageFree_Shipment_Score'])['Confidence_Score'])
    
    return sample_df
    


# In[86]:


Rating_agg=Rating.groupby(['Customer','Transporter']).apply(weight_agg)


Rating_agg=Rating_agg.groupby(['Customer','Transporter']).agg({'Total_Indent_Count':'mean',
                                                               'Placement_Rating':'sum',
                                                               'Delivery_Rating':'sum',
                                                               'DamageFree_Shipment_Rating':'sum',
                                                               'Placement_Probability':'sum',
                                                               'On-Time_Delivery_Probability':'sum',
                                                               'DamageFree_Shipment_Score':'sum'})

 

Rating_agg=Rating_agg.reset_index()


 


Rating_agg['Total_Indent_Count']=Rating_agg['Total_Indent_Count'].apply(lambda x : round(x,2))
Rating_agg['Placement_Rating']=Rating_agg['Placement_Rating'].apply(lambda x : round(x,2))
Rating_agg['Delivery_Rating']=Rating_agg['Delivery_Rating'].apply(lambda x : round(x,2))
Rating_agg['DamageFree_Shipment_Rating']=Rating_agg['DamageFree_Shipment_Rating'].apply(lambda x : round(x,2))


Rating_agg['Placement_Probability']=Rating_agg['Placement_Probability'].apply(lambda x : round(x,2))
Rating_agg['On-Time_Delivery_Probability']=Rating_agg['On-Time_Delivery_Probability'].apply(lambda x : round(x,2))
Rating_agg['DamageFree_Shipment_Score']=Rating_agg['DamageFree_Shipment_Score'].apply(lambda x : round(x,2))


# In[91]:


Indent_per_Customer=Rating_agg.groupby(['Customer']).agg({'Total_Indent_Count':'sum'}).reset_index()
Indent_per_Customer.columns=['Customer','Total_Indent_Count_PerCustomer']
Rating_agg=pd.merge(Rating_agg,Indent_per_Customer,how='inner',on=['Customer'])


Rating_agg=Rating_agg[['Customer', 'Transporter', 'Total_Indent_Count','Total_Indent_Count_PerCustomer','Placement_Rating',
       'Delivery_Rating', 'DamageFree_Shipment_Rating',
       'Placement_Probability', 'On-Time_Delivery_Probability',
       'DamageFree_Shipment_Score']]

 

Rating_agg.insert(4,column='Confidence_Score',value=round(np.log(Rating_agg['Total_Indent_Count'])/np.log(Rating_agg['Total_Indent_Count_PerCustomer']),2))
     
    
 

Rating=Rating.drop(['Total_Indent_Count'],1)
Rating_agg=Rating_agg.drop(['Total_Indent_Count_PerCustomer'],1)

 

Rating.to_csv(os.path.join(model_out_path,'Combined_output','Rating_alpha_0.1.csv'),index=False)
Rating_agg.to_csv(os.path.join(model_out_path,'Combined_output','Rating_agg_0.1.csv'),index=False)
 

Rating.columns=['Customer',
'Source',
'Destination',
'Transporter',
'Vehicle Type',
'contract_type',
'Cost_Kg',
'Indent_Count_LaneVT',
'Confidence_Score',
'Placement_Rating',
'Delivery_Rating',
'DamageFree_Shipment_Rating',
'Placement_Probability',
'On_Time_Delivery_Probability',
'DamageFree_Shipment_Score']


 

Rating_agg.columns=['Customer',
'Transporter',
'Total_Indent_Count',
'Confidence_Score',
'Placement_Rating',
'Delivery_Rating',
'DamageFree_Shipment_Rating',
'Placement_Probability',
'On_Time_Delivery_Probability',
'DamageFree_Shipment_Score'] 


Rating.to_csv(os.path.join(model_out_path,'Combined_output','Rating.csv'),index=False)
s3 = boto3.resource('s3')
BUCKET = "pando-labs-datamigration"
s3.Bucket(BUCKET).upload_file(os.path.join(model_out_path,'Combined_output','Rating.csv'), "Transporter_Scores/Rating.csv")



Rating_agg.to_csv(os.path.join(model_out_path,'Combined_output','Rating_agg.csv'),index=False)
s3 = boto3.resource('s3')
BUCKET = "pando-labs-datamigration"
s3.Bucket(BUCKET).upload_file(os.path.join(model_out_path,'Combined_output','Rating_agg.csv'), "Transporter_Scores/Rating_agg.csv")

 


 



