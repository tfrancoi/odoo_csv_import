#!/usr/bin/env bash
#Need to launch odoo database accessible with the configuration given in conf/connection.conf
#test works well on V11
#Modules contacts need to be installed
#EXEC="python2"
for EXEC in "python2" "python3" "python3.7" "coverage run -a"
do
  echo "============== Test $EXEC =============="
  rm -rf data
  mkdir data
  export PYTHONPATH=../
  echo "> Erase"
  coverage erase
  echo "> Generate data for import"
  $EXEC test_import.py "$EXEC"
  echo "> Run test import"
  sh 0_partner_generated.sh
  echo "> Run test split file"
  $EXEC test_split.py "$EXEC"
  echo "> Test mapping from file"
  $EXEC test_from_file.py "$EXEC"
  echo "> Import data with error"
  sh 2_contact_import.sh 2> error.log
  echo "> Import Product"
  $EXEC test_product_v9.py "$EXEC"
  sh 3_product_import.sh
  echo "> Import Product v10"
  $EXEC test_product_v10.py "$EXEC"
  sh 4_product_import.sh
  sh 5_partner_export.sh "$EXEC"
  echo "> Import One2Many"
  sh 6_o2m_import.sh "$EXEC"
  echo "> Convert Binary"
  sh 7_convert_binary.sh "$EXEC"
  echo "Test join"
  $EXEC test_merge.py
  coverage html
done
