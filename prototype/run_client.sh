CN=6
NN=10
COR_IP='0.0.0.0'
STRIPE_NUM=10
XX=2

KK=6
LL=3
G_M=2

K1=3
M1=1
K2=2
M2=1

ET1='LPC'
MPT1='Vertical'
ET2='Azure_LRC'
MPT2='DIS'
# VALUE_SIZE=24576   #24MB, 4MB, k = 6
# VALUE_SIZE=49152   #48MB, 8MB, k = 6
# VALUE_SIZE=98304   #96MB, 16MB, k = 6
# VALUE_SIZE=196608  #192MB, 32MB, k = 6
VALUE_SIZE=393216  #384MB, 64MB, k = 6
# VALUE_SIZE=524288  #512MB, 64MB, k = 8
# VALUE_SIZE=786432  #768MB, 64MB, k = 12 / 128MB, k = 6

# run client
./project/cmake/build/run_client ${CN} ${NN} ${COR_IP} ${ET1} ${MPT1} false ${STRIPE_NUM} ${VALUE_SIZE} ${XX} ${K1} ${M1} ${K2} ${M2} false

# unlimit bandwidth
sh exp.sh 4
# kill datanodes and proxies
sh exp.sh 0
# kill coordinator
pkill -9 run_coordinator