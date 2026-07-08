
all:
	docker build -t cronstable-pyinstaller-build -f Dockerfile ..
	docker run --name cronstable-pyinstaller-build  --rm -d cronstable-pyinstaller-build sleep infinity
	mkdir -p dist
	docker cp cronstable-pyinstaller-build:/root/cronstable/dist/cronstable dist/
	docker kill cronstable-pyinstaller-build
	ls -sh dist/cronstable
	dist/cronstable --version
