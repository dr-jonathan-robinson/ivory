MFLAGS = -s
MAKEFLAGS = $(MFLAGS)
VERSION=0.11.1
MKDOCS=../target/mkdocs-${VERSION}
BIN=target/mkdocs-${VERSION}/bin/mkdocs

.PHONY: build watch

default: watch

../target:
	mkdir ../target

${MKDOCS}: ../target
	cd ../target && curl https://pypi.python.org/packages/source/m/mkdocs/mkdocs-${VERSION}.tar.gz | tar xz

${BIN}: ${MKDOCS}
	cd ${MKDOCS} && python setup.py build
	cd ${MKDOCS} && python setup.py install --user --install-scripts=bin

build: ${BIN}
	cd ../ && ${BIN} build --clean

watch: ${BIN}
	cd ../ && ${BIN} serve
