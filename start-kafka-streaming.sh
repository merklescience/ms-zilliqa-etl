#!/usr/bin/bash
source /root/zilliqa-etl/bin/activate
doppler run -- zilliqaetl stream --provider-uri $ZILLIQA_PROVIDER_URI -o kafka -t producer-zilliqa -ts hot -l /root/zilliqa-etl/last_synced_block.txt