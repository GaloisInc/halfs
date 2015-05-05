The HAskelL File System
===

Intended to be used with HaLVM.

## description
A library implementing a file system suitable for use in HaLVMs.  Provides useful abstractions over the underlying block layer.  Implemented atop FUSE.  Note: This is a new implementation of the halfs project, and bears little to no resemblance to halfs 0.2.

## Get started
ATTENTION: I tested it with `cabal`, i.e. the `cabal` on your host OS, instead of the `halvm-cabal`.

First, install HFuse package (with `devel` version) into your host OS with any package manager, for example:

```
yum install fuse fuse-devel
```

Then go to `deps/hfuse`, `cabal install` the `hfuse` dependency. This package has an [upstream](https://github.com/m15k/hfuse).

Finally, go back to topdir, `cabal configure`, then `cabal build`.

You can test it with the built `./halfs-tests` under `dist/build/halfs-tests`