module.exports = {
  apps: [
    {
      name: 'sn4.miner',
      script: 'neurons/miner.py',
      args: '--wallet.name default --netuid 40 --wallet.hotkey default --subtensor.network test --model-endpoint http://172.65.175.60:3000/v1 --api-key sk-DtT3YuhbLJ8NLc7n730cFbA584794b5890C30e173859A00e --axon.port 40001 --axon.external_ip 64.21.190.93 --logging.debug --logging.logging_dir /Users/tutu/Documents/Github/targon/logs',
      interpreter: '/Users/tutu/Documents/Github/targon/.venv/bin/python3',
      env: {
          PYTHONPATH: '/Users/tutu/Documents/Github/targon:$PYTHONPATH',
          TZ: 'UTC'
      },
      out_file: '/Users/tutu/Documents/Github/targon/logs/miner1/output.log',
      error_file: '/Users/tutu/Documents/Github/targon/logs/miner1/error.log'
    },
  ]
}