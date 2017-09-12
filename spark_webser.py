#!/usr/bin/env python
# -*- coding:utf-8 -*-

import ConfigParser
import subprocess

import time
import tornado.ioloop
import tornado.web

spark_cmd_path = None
spark_cmd_jar = None
spark_cmd_HDFSurl = None
spark_cmd_simires_path = None
spark_cmd_anyou_dir = None


class SimiMatSparkHandler(tornado.web.RequestHandler):
    def get(self):
        pass

    def post(self):
        try:
            anyou_simi = self.get_argument('anyousimi')
            anyou_type = self.get_argument('anyoutype')
            simi_flag = self.get_argument('simiflag')

            if not (spark_cmd_path and spark_cmd_jar and spark_cmd_HDFSurl and
                        spark_cmd_simires_path and spark_cmd_anyou_dir):
                print 'no spark cmd config'
                return

            print '---link server---'

            # spark-submit
            # /home/uww/Work/server/spark-2.1.1-bin-hadoop2.7/bin/spark-submit --master yarn --deploy-mode cluster
            # --name testScala /home/uww/Work/data/sparkdata/testSerAlpSave.jar
            # hdfs://172.23.4.87:9000 /user/uww/spark/result/***.txt
            # hdfs://ubuntu:9000/user/uww/spark/data/ simimat.test 9137 9139 9133 9130
            spark_cmd = spark_cmd_path + ' --master yarn --deploy-mode cluster --name sparkSimiMat ' + \
                        spark_cmd_jar + ' ' + spark_cmd_HDFSurl + ' ' + spark_cmd_simires_path + simi_flag + '.txt ' + \
                        spark_cmd_HDFSurl + spark_cmd_anyou_dir + ' ' + anyou_simi + ' ' + anyou_type

            print spark_cmd
            spark_simi_p = subprocess.Popen(spark_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            spark_simi_p.communicate()
            # os.popen(vocab_count_cmd)
            time.sleep(0)

            HDFS_ext_cmd = 'hadoop fs -get ' + spark_cmd_HDFSurl + spark_cmd_simires_path + simi_flag + '.txt ' + \
                           './data'
            print HDFS_ext_cmd
            HDFS_ext_p = subprocess.Popen(HDFS_ext_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            HDFS_ext_p.communicate()
            time.sleep(0)

            file_simi = open('./data/' + simi_flag + '.txt', 'r')
            simi_data = file_simi.readline()
            file_simi.close()
            self.write(str(simi_data))

            # HDFS_del_cmd = 'hadoop fs -rm ' + spark_cmd_HDFSurl + spark_cmd_simires_path + simi_flag + '.txt '
            # print HDFS_del_cmd
            # HDFS_del_p = subprocess.Popen(HDFS_del_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            # HDFS_del_p.communicate()
            # time.sleep(0)

            file_simi_del_cmd = 'rm -rf ./data/' + simi_flag + '.txt'
            print file_simi_del_cmd
            file_simi_del_p = subprocess.Popen(file_simi_del_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                               shell=True)
            file_simi_del_p.communicate()
            time.sleep(0)

            print '---ok---'
        except Exception, e:
            print Exception, ':', e
            self.write(str(e))


if __name__ == "__main__":
    print '---start server---'
    cp = ConfigParser.SafeConfigParser()
    cp.read('sparkservice.conf')

    spark_cmd_path = cp.get('sparkCmd', 'sparkPath')
    spark_cmd_jar = cp.get('sparkCmd', 'simiCmptJar')
    spark_cmd_HDFSurl = cp.get('sparkCmd', 'HDFSurl')
    spark_cmd_simires_path = cp.get('sparkCmd', 'simiResPath')
    spark_cmd_anyou_dir = cp.get('sparkCmd', 'anyouDir')

    server_port = cp.get('service', 'port')

    application = tornado.web.Application([
        (r'/simiMatSpark', SimiMatSparkHandler)
    ])

    application.listen(server_port)
    tornado.ioloop.IOLoop.instance().start()
