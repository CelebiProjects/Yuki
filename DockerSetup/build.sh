#!/usr/bin
# get the date tag
tag=$(date +%Y%m%d)

rm -rf Yuki
cp -r ../../Yuki .
rm -rf Yuki/dist
rm -rf Yuki/Yuki.egg-info
docker build . -t yuki-nightly-0.0.${tag}-1
docker save -o yuki-nightly-0.0.${tag}-1.tar yuki-nightly-0.0.${tag}-1

