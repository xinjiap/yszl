'''
Author: Iris Peng. Date: Feb 21, 2016
Usage: Scrape Weibo posts from Zhongsou for the first time for a query

In the terminal, type
$ python3 scrape_weibo.py

and follow the prompts

'''
import requests
from bs4 import BeautifulSoup
from pandas import DataFrame
import time
import pandas
import glob, os


global QUERY_LINK
QUERY_LINK = 'http://t.zhongsou.com/wb?form_id=1&org=1&sel=0&so=1&v=%D6%D0%CB%D1&w=%B1%C6'#link

global OUTPUT_FILE_NAME
OUTPUT_FILE_NAME = 'scrape' # Name of your output file

global WORKING_DIR
WORKING_DIR = '~/Corpora/'

global OLD_MASTER_FILE
OLD_MASTER_FILE = '{}Text_data/'.format(WORKING_DIR) + 'yeshizuile.txt' #Feed the new output
    

class NewScrape():
    
    def scrape_main(self):
        '''
        Top-level function.
        Use links from below, scrape a page, sleep for 5s, and restart on the next link.
        '''
        for i in self.gen_links():
            index = str(self.gen_links().index(i))
            link = i
            self.get_weibo(link,index)
            time.sleep(5)
        
        self.retrieve_posts(OUTPUT_FILE_NAME)
        #self.clean_temp()
        print('='*10)
        print('Congratulations! Your data is stored')
        return None

    def gen_links(self):
        links = []
        for i in range(1,51):
            i = str(i)        
            links.append('{}&b={}'.format(QUERY_LINK,i))
        return links

    def get_weibo(self,link,index):
        
        '''
        Scrape a certain weibio search result page on 'zhongsou' and store it in locally.
        '''

        html_doc = open('{}Temp/weibo.txt'.format(WORKING_DIR),'w', encoding = 'utf8')
       
        r = requests.get(link)
        print ('accessing web data.')
        html_doc.write(r.text)
        html_doc.close()
        
        # Write into a csv file
        outfile_name = 'zhongsou_results_page_' + index + '.csv'
        outfile = open('{}Temp/'.format(WORKING_DIR) + outfile_name,'w', encoding = 'utf8') #change path
        
        # Turn the text into a BeautifulSoup object and strip down the text.
        html_doc = open('{}Temp/weibo.txt'.format(WORKING_DIR),'r', encoding = 'utf8')#change path
        soup = BeautifulSoup(html_doc)

        user_link = []
        post_txt = []
        post_link = []
        post_time = []
        
        weibo_items = soup.find_all('div', class_='weibo_item')
        
        for item in weibo_items: 
                
            for link in item.find_all('a', target='_blank', class_='sina_weibo'):
                url = link.get('href')
                post_link.append(url)

            for post in item.find_all('h3', class_='weibo_title'):
                for a in post.find_all('a'):
                    url = a.get('href')
                user_link.append(url)

            for time in item.find_all('div', class_='weibo_time'):
                txt = time.get_text()
                post_time.append(txt)

            for post in item.find_all('p', class_='weibo_txt'):
                txt = post.get_text()
                post_txt.append(txt)
            
        data = {'post_text':post_txt,'post_link':post_link,'user':user_link, 'time':post_time}
        frame = DataFrame(data)
        frame.to_csv(outfile, encoding='utf-8')
        print (outfile_name,'processed complete.')
        
        outfile.close()
        html_doc.close()
        return None

    def clean_temp(self):
        filelist = glob.glob('{}Temp/*'.format(WORKING_DIR))
        for f in filelist:
            os.remove(f)
        print('Temp files removed')
        return None

        
    def retrieve_posts(self,outfile_name):
        '''(str)->a file
        '''        
        post_text = []
        
        for i in range(50):
            frame_2 = pandas.read_csv('{}Temp/zhongsou_results_page_{}.csv'.format(WORKING_DIR, str(i)))#change directory
            df2 = DataFrame(frame_2)
            for i in df2.post_text:#the column'post_text'
                post_text.append(i)

        data = {'post_text':post_text}
        frame = DataFrame(data)
        frame.to_csv('{}Text_data/{}.txt'.format(WORKING_DIR, outfile_name), encoding = 'utf-8')#change saved path
        frame.to_excel('{}Text_data/{}.xlsx'.format(WORKING_DIR, outfile_name), encoding = 'utf-8')#change saved path
        print("Done")
        return None      

class ContinueScrape():
    
    def scrape_main(self):
        '''
        Top-level function.
        Use links from below, scrape a page, sleep for 5s, and restart on the next link.
        '''
        for i in self.gen_links():
            index = str(self.gen_links().index(i))
            link = i
            cmd = self.get_weibo(link,index)
            if cmd == 'STOP':
                break
            else:
                time.sleep(10)
                continue
            
        print('='*10)
        print('Scrape is now complete. Help me to organize them.')
        print ('View your temp folder, what is the biggest number of the files? \n')
        fn = int(input())
        self.retrieve_posts(fn)
        print('='*10)
        print('Congratulations! Your data is stored')
        return 

    def gen_links(self):
        links = []
        for i in range(1,51):
            i = str(i)        
            links.append('{}&b={}'.format(QUERY_LINK,i))
        return links

    def get_weibo(self,link,index):
        
        '''
        Scrape a certain weibio search result page on 'zhongsou' and store it in locally.
        '''

        html_doc = open('{}Temp/weibo.txt'.format(WORKING_DIR), 'w', encoding='utf8')

        r = requests.get(link)
        print ('Accessing web data.')
        html_doc.write(r.text)
        html_doc.close()

        # Retrieve scrape history
        h_post_text = [] 
        h_frame = pandas.read_csv(OLD_MASTER_FILE)    
        h_df = DataFrame(h_frame)
        for i in h_df.post_text:
            h_post_text.append(i)
        
        # Write into a csv file
        outfile_name = 'zhongsou_results_page_' + index + '.csv'
        outfile = open('{}Temp/'.format(WORKING_DIR)+ outfile_name,'w', encoding = 'utf8') #change path
        
        # Turn the text into a BeautifulSoup object and strip down the text.
        html_doc = open('{}Temp/weibo.txt'.format(WORKING_DIR), 'r', encoding='utf8')
        soup = BeautifulSoup(html_doc)

        user_link = []
        post_txt = []
        post_link = []
        post_time = []
        cmd = None
        
        weibo_items = soup.find_all('div', class_='weibo_item')
        
        for item in weibo_items: 
                
            for link in item.find_all('a', target='_blank', class_='sina_weibo'):
                url = link.get('href')
                post_link.append(url)

            for post in item.find_all('h3', class_='weibo_title'):
                for a in post.find_all('a'):
                    url = a.get('href')
                user_link.append(url)

            for time in item.find_all('div', class_='weibo_time'):
                txt = time.get_text()
                post_time.append(txt)

            for post in item.find_all('p', class_='weibo_txt'):
                txt = post.get_text()
                post_txt.append(txt)

            #has bugs!
            #if txt in h_post_text:
            if txt == h_post_text[0]:    
                print (txt)
                print(' ___ exists')
                print ('End of new data.') #Doesn't affect main function, break should be in main function
                del post_link[-1]
                del user_link[-1]
                del post_time[-1]
                del post_txt[-1]
                cmd = 'STOP'
                break
            
        data = {'post_text':post_txt,'post_link':post_link,'user':user_link, 'time':post_time}
        frame = DataFrame(data)
        frame.to_csv(outfile, encoding='utf-8')
        print (outfile_name,'processed complete.')
        
        outfile.close()
        html_doc.close()
        return cmd

    def retrieve_posts(self,file_number_total):
        '''(int)->a file
        '''
        post_text = []
        
            
        for i in range(file_number_total+1):
            frame_2 = pandas.read_csv('{}Temp/zhongsou_results_page_{}.csv'.format(WORKING_DIR, str(i)))
            df2 = DataFrame(frame_2)
            for i in df2.post_text:#the column'post_text'
                post_text.append(i)

        frame_1 = pandas.read_csv(OLD_MASTER_FILE)
        df1 = DataFrame(frame_1)
        for i in df1.post_text:
            post_text.append(i)

        data = {'post_text':post_text}
        frame = DataFrame(data)
        frame.to_csv('{}Text_data/{}_2.txt'.format(WORKING_DIR, OUTPUT_FILE_NAME), encoding = 'utf-8')#saved path
        frame.to_excel('{}Text_data/{}_2.xlsx'.format(WORKING_DIR, OUTPUT_FILE_NAME), encoding = 'utf-8')#saved path


        print("Data gathered.")

##        filelist = glob.glob('{}Temp/*'.format(WORKING_DIR))
##        for f in filelist:
##            os.remove(f)

        #os.remove(OLD_MASTER_FILE)

        print('Temp files removed')

        return None 

print('='*10)
print('This program will help you collect Weibo language data as generated by the 中搜 search results.\n')
print('Use this page to generate a link for your query item:\n\nhttp://t.zhongsou.com/wb?form_id=1&org=1&sel=0&so=1&v=%D6%D0%CB%D1&w=%CD%F8%D3%EF')
QUERY_LINK = input('\nPaste your query link \n> ')
OUTPUT_FILE_NAME = input('\nWhat\'s your query term? (This will be used as file name)\n> ')
resp = input('\nIs this your first time running this query? Y/N\n> ').upper()
if resp == 'Y':
    print()
    print('='*10)
    print('Initialize scraping now.')
    print('='*10)
    NewScrape().scrape_main()
elif resp == 'N':
    OLD_MASTER_FILE = input('\nWhere is the old txt file you want to merge later? Please paste full path. \n> ')
    print()
    print('='*10)
    print('WARNING: FURTHER ACTIONS NEEDED AT THE END OF SCRAPING.')
    print('Initialize scraping now.')
    print('='*10)
    ContinueScrape().scrape_main()
    
else:
    print('Invalid command. Try again.')
