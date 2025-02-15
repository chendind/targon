script = '/mnt/workspace/targon/neurons/miner.py'
interpreter = '/mnt/workspace/targon/.venv/bin/python'
PYTHONPATH = '/mnt/workspace/targon:$PYTHONPATH'
TZ = 'UTC'
REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6479
REDIS_DB = 3
REDIS_PASSWORD = 'your_redis_password'
TAOCD_LOGER_PATH = '/mnt/workspace/targon/logs'
TAOCD_PWD = '/mnt/workspace/targon'
HOST_IP = '64.21.190.81'

const appConfig = Array.from({length:5}).reduce((pre,cur,index)=>{
  const coldGroup = Array.from({length:40}).map((v,i)=>{
    const cdNum = index+1
    const zeroNum = String(i + 1).padStart(2, '0')
    const port = i < 20 ? (9500 + index * 20 + i + 1) : (9600 + index * 20 + i + 1 - 20)
    const taocdName = `taocd19_${cdNum}.taocd${cdNum}_${zeroNum}`
    const coldkey = `taocd19_${cdNum}`
    const hotkey = `taocd${cdNum}_${zeroNum}`
    return {
      name: taocdName,
      script: script,
      args: `--wallet.name ${coldkey} --wallet.hotkey ${hotkey} --subtensor.network finney --netuid 4 --model-endpoint http://127.0.0.1:3000/v1 --api-key sk-DtT3YuhbLJ8NLc7n730cFbA584794b5890C30e173859A00e --axon.port ${port} --axon.external_ip ${HOST_IP} --logging.debug --logging.logging_dir ${TAOCD_LOGER_PATH}/${taocdName}`,
      interpreter,
      env: {
          PYTHONPATH,
          TZ,
          REDIS_HOST,
          REDIS_PORT,
          REDIS_DB,
          REDIS_PASSWORD,
          TAOCD_LOGER_PATH: `${TAOCD_LOGER_PATH}/${taocdName}`,
          HOST_IP
      },
      out_file: `${TAOCD_LOGER_PATH}/${taocdName}/output.log`,
      error_file: `${TAOCD_LOGER_PATH}/${taocdName}/error.log`
    }
  }).filter(v=>v)
  return [...pre,...coldGroup]
},[]).filter(v=>v)

module.exports = {
    apps: appConfig
}