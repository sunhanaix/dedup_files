import os, sys, re, json, time
import hashlib
from collections import defaultdict
import ast
import util
sys.stdout.reconfigure(encoding='utf-8')

SUPPORT_HARD_LINK_FS=['ext2','ext2','ext4','xfs','zfs','ntfs'] #fat和exfat文件系统，都不支持硬链接

#本程序，扫描给定目录及其子目录的所有文件，并计算每个文件的md5值（假定md5没有碰撞），结果存入md5_dict

app_path = os.path.dirname(os.path.abspath(sys.argv[0]))
log_file = os.path.basename(sys.argv[0]).split('.')[0] + '.log'
cfg_file=os.path.basename(sys.argv[0]).split('.')[0] + '.ini'

md5_dict=defaultdict(list)
md5_array=[]

mylog=util.get_logger(log_file) #设置写入日志函数
cfg=util.get_cfg(cfg_file) #从配置文件中读入配置，如果文件不存在，则创建一个
mylog.info(json.dumps(cfg,indent=2,ensure_ascii=True))

#检查配置的去重目录是否支持硬链接，如果不支持，进行警告提醒，并直接退出
for dir in cfg['dirs']:
    fs_type=util.get_fs_type(dir)
    if fs_type in SUPPORT_HARD_LINK_FS:
        mylog.info(f"{dir}所在文件系统为{fs_type}，支持硬链接")
    else:
        print()
        mylog.error(f"{dir}所在文件系统为{fs_type}，不支持硬链接，请重新调整{cfg_file}中dirs的设置")
        print()
        input("按回车键退出")
        sys.exit(-1)

def write_cache_file(cache_data): #把所有的内存中的每个文件的md5的数据，按一条条格式，全部dump到cache_file里面去
    with open(cfg['cache_file'], 'w', encoding='utf-8') as file:
        for entry in cache_data:
            # 使用repr()将字典转换为字符串，并写入文件
            file.write(repr(entry) + '\n')

def append_record_to_cache(record_data): #把当前这么一条数据，按照字典一行的模式，追加到cache_file中去
    # 使用'a'模式打开文件，以便追加数据
    with open(cfg['cache_file'], 'a', encoding='utf-8') as file:
        # 使用repr()将字典转换为字符串，并追加到文件末尾
        file.write(repr(record_data) + '\n')

def read_cache_file(): #把cache中所有文件的md5的数值，全部读入到内存中来，返回cache_data的数组
    cache_data = []
    if not os.path.isfile(cfg['cache_file']):
        return []
    with open(cfg['cache_file'], 'r', encoding='utf-8') as file:
        for line in file:
            # 使用ast.literal_eval将文本行解析为字典
            data = ast.literal_eval(line.strip())
            cache_data.append(data)
    return cache_data

def convert_array_to_dict(input_array): #把md5的{'file_name':'md5_value'}数组，转换成基于每一个md5的hash dict中，dict的key为md5值
    result_dict =defaultdict(list)
    for item in input_array:
        for filename, uuid in item.items():
            if uuid not in result_dict:
                result_dict[uuid] = [filename]
            else:
                result_dict[uuid].append(filename)
    return result_dict

def convert_dict_to_array(input_dict): #把基于md5做hash key的dict，转换为md5的{'file_name':'md5_value'}数组
    result = []
    for uuid, filenames in input_dict.items():
        for filename in filenames:
            result.append({filename: uuid})
    return result

def find_record_by_file_path(file_path):
    #给定一个file_path，检查是否已经在md5_array列表里面，如果在的话，返回True，否则False
    #如果没缓存过，或者没变化，会返回None
    global md5_array
    for record in md5_array:
        if file_path in record:
            return True
    return None

def get_md5_info(): #遍历所有指定目录，获取每一个文件名字，然后对每一个文件，进行md5计算，并记录到cache中
    # 计算文件的MD5哈希值并保存到字典中
    # 用于存储文件MD5哈希值和路径的字典
    global md5_array,md5_dict
    i=0
    for directory in cfg['dirs']:
        mylog.info(f"遍历{directory}获得每一个文件中……")
        for root, _, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)
                i+=1
                ss_file_path=util.remove_unprintable_chars(file_path)
                log_ss=f"{i=},{ss_file_path},"
                if find_record_by_file_path(file_path):
                    mylog.info(f"{log_ss} in cache,will pass")
                    continue
                mylog.info(f"{log_ss} not in cache,will calc md5 now")
                md5_value=None
                try:
                    md5_value=util.md5_file(file_path)
                    md5_dict[md5_value].append(file_path)
                    md5_array.append({file_path:md5_value})
                    append_record_to_cache({file_path:md5_value})
                except Exception as e:
                    mylog.error(f"try open file:{ss_file_path} failed,reason:{e}")
                    mylog.info(f"{md5_value=},{md5_dict=},{md5_array=}")
                    continue
    open(cfg['md5_key_file'],'w',encoding='utf8').write(json.dumps(md5_dict,indent=2,ensure_ascii=False))
    return md5_dict

#从cache文件中，载入已经算过的md5数据
md5_array=read_cache_file()
md5_dict = convert_array_to_dict(md5_array)

#开始计算每一个文件的md5
md5_dict = get_md5_info()
mylog.info("遍历所有目录完成，现在比对重复文件")

if cfg['ask_before_del']:
    a=input("将要删除副本>1的文件，是否继续？(Y/N)")
    if not a.lstrip().rstrip().lower()=='y':
        mylog.info("你没有选择Y，本程序将会退出，不进行删除副本操作")
        print()
        input("按回车键退出")
        sys.exit()


# 创建硬链接
for md5_value, file_paths in md5_dict.items():
    if len(file_paths) > 1:
        # 找到重复的文件，保留一个，删除其它的，然后创建硬链接
        reference_file = file_paths[0]
        for duplicate_file in file_paths[1:]:
            try:
                mylog.warn(f"deleting {duplicate_file}")
            except  Exception as e:
                mylog.error(e)
                continue
            try:
                os.remove(duplicate_file)
            except Exception as e:
                mylog.error(f"remove file failed,reason:{e}")
                continue
            try:
                mylog.info(f"link from {reference_file} to {duplicate_file}")
            except Exception as e:
                mylog.error(e)
                continue
            try:
                os.link(reference_file, duplicate_file)
            except Exception as e:
                mylog.error(f"link file failed,reason:{e}")
                continue
print()
input("按回车键退出")
