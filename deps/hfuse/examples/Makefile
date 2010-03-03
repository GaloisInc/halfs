all: HelloFS BindFS

HelloFS: HelloFS.hs
	ghc -threaded --make HelloFS.hs

BindFS: BindFS.hs
	ghc -threaded --make BindFS.hs

LiveFS: LiveFS.hs
	ghc -threaded --make LiveFS.hs

clean:
	rm -f *hi *o BindFS HelloFS LiveFS
