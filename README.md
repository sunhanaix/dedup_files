# dedup_files   
for file deduplication（文件去重）      
本程序的原理是扫描目标目录的所有文件，计算他们的md5值，如果一样，说明是一个文件。   
统计出来所有md5值后，找到副本数量大于1的文件，只保留一份副本，然后用这份副本做硬链接到其它的几个副本文件名处。   
从而尝试释放空间。   
**由于FAT/FAT32/EXFAT等文件系统不支持硬链接，所以这样的文件系统是不被支持的，也就不用尝试了。**   
（当前一般windows都是ntfs文件系统，支持硬链接）   
最初写来是用来给微信数据文件夹瘦身的，因为微信的各种文件转发，图片转发什么的，都是copy，不是hard link。   
**注：不知个人微信在哪个版本开始，文件/图片转发，已经从copy改成hard link了。从当前测试的win10上的微信3.9.7.25版本看，转发时，微信会智能做硬链接了。**   
当然这么久了，肯定有历史积累，有历史包袱，历史包袱部分，还都是副本，不是硬链接，用本程序还是可以去重   
对于我个人而言，改成hard link后，个人的30G占用空间，一下子就少了10G。   
当然非微信的目录也是一样的可以文件去重。第一次执行时，它会自动生成一个dedup_files.ini的配置文件，   
其中dirs里面指定了哪些目录是要扫描进行去重的，   
cache_file：参数指定存放cache文件的路径，默认放在当前程序路径下    
md5_key_file：以md5为key的hash dict，文件存放路径   
to_del_file:  以md5为key的hash dict，放置了后面要删除文件改用硬链接的文件名称   
ask_before_del：删除文件前，是否进行询问   
max_workers：算md5时的，最大并发线程数量   


增加了统计预计可以释放的空间大小（自动忽略已经做了硬链接的文件），要副本改硬链接的文件数量，以及实际执行完后释放了的空间大小。   
增加了多线程并发，多个文件同时并发计算md5，加快速度。但python的GIL限制，也就能快个2-3倍左右   
