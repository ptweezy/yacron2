
all:
	docker build -t yacron2-pyinstaller-build -f Dockerfile ..
	docker run --name yacron2-pyinstaller-build  --rm -d yacron2-pyinstaller-build sleep infinity
	mkdir -p dist
	docker cp yacron2-pyinstaller-build:/root/yacron2/dist/yacron2 dist/
	docker kill yacron2-pyinstaller-build
	ls -sh dist/yacron2
	dist/yacron2 --version
