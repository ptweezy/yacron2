
all:
	docker build -t yacron2-pyinstaller-build -f Dockerfile ..
	docker run --name yacron2-pyinstaller-build  --rm -d yacron2-pyinstaller-build sleep infinity
	mkdir -p dist
	docker cp yacron-pyinstaller-build:/root/yacron/dist/yacron dist/
	docker kill yacron-pyinstaller-build
	ls -sh dist/yacron
	dist/yacron --version

all-alpine:
	docker build -t yacron2-pyinstaller-build -f Dockerfile-alpine ..
	docker run --name yacron2-pyinstaller-build  --rm -d yacron2-pyinstaller-build sleep infinity
	mkdir -p dist
	docker cp yacron-pyinstaller-build:/root/yacron/dist/yacron dist/
	docker kill yacron-pyinstaller-build
	ls -sh dist/yacron
	dist/yacron --version
