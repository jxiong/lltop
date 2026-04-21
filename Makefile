all: lltop llostio

lltop: lltop.py util.py
	python3 -m PyInstaller --onefile --specpath build lltop.py
	cp dist/lltop lltop
	chmod +x lltop

llostio: llostio.py util.py
	python3 -m PyInstaller --onefile --specpath build llostio.py
	cp dist/llostio llostio
	chmod +x llostio

rpm:
	mkdir -p ~/rpmbuild/{BUILD,RPMS,SOURCES,SPECS,SRPMS}
	tar --exclude='.git' --exclude='build' --exclude='dist' --exclude='__pycache__' --transform 's,^\.,lltop-1.0.0,' -czf ~/rpmbuild/SOURCES/lltop-1.0.0.tar.gz .
	cp lltop.spec ~/rpmbuild/SPECS/
	rpmbuild -ba ~/rpmbuild/SPECS/lltop.spec

clean:
	rm -f lltop llostio
	rm -rf build dist
