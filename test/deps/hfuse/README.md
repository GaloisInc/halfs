# Haskell FUSE API

Filesystem in Userspace ("FUSE") makes it possible to implement a filesystem as a userspace program.

This library is the Haskell binding to this library.

## License

[BSD 3-Clause](./LICENSE)

## Information

- Programs using HFuse should be compiled with -threaded.
- This now works for base 4.6+
- Added build options support for FreeBSD (contribution by https://github.com/pesco)
- MacFUSE should also work (https://github.com/mwotton/hfuse)
- [OSXFuse](https://osxfuse.github.io/) also works (https://github.com/edyu/hfuse)

## Installation

All of the usual methods for installation will be supported.
For Mac OS X, you must install [OSXFuse](https://osxfuse.github.io/) first.

**Installation via Hackage**

```
cabal install hfuse
```

**Installation for development**

Can either be handled via [Hackage](http://hackage.haskell.org/packages/search?terms=hfuse)

```
cabal unpack hfuse
cd HFuse-0.2.4.4
cabal sandbox init
cabal install --only-dependencies
cabal install -fdeveloper
```

Or the library can be installed via Github [repo][2]

```
git clone git://github.com/m15k/hfuse
cd hfuse
cabal sandbox init
cabal install --only-dependencies
cabal install -fdeveloper
```

**NOTE!**

* To use the sandboxes feature in Cabal your version must be higher than 1.18. *highly recommended*

## Development

To get a feel for HFuse, there are a number of example applications.  They can be built by supplying the `-fdeveloper` [configuration flag][3] to Cabal.

> git clone https://github.com/m15k/hfuse

## Examples

[HelloFS](./examples/HelloFS.hs) is as basic as you get.  Haskell version of the canonical [example](http://fuse.sourceforge.net/helloworld.html) from the FUSE project.  Once compiled here is how you run HelloFS.

```
$ mkdir ~/fuseTest
$ ./HelloFS ~/fuseTest
```

This creates a file in the *fuseTest* directory.  Now to test the application.

```
$ cat ~/fuseTest/hello
Hello World, HFuse!
```

To unmount issue the following command:

```
$ fusermount -u ~/fuseTest
```

## Other Samples

There are other projects on hackage which use HFuse as a dependency.  Check [these](http://packdeps.haskellers.com/reverse/HFuse) out for a possibly richer experience than what is included with the [examples](./examples) folder.

If you lack for inspiration the FUSE [Wiki](http://sourceforge.net/p/fuse/wiki/FileSystems/) have amassed quite the list of links to downstream projects.

## Contributions

Help is always welcome.  Pull requests are appreciated.

If you run into any problems or bugs, please report the issue on [Github][1]

## RoadMap

I would like to create the following examples:

- MemoryFS.hs := In-memory file system
- VBoxFS.hs := Mount VirtualBox disks as filesystem
- SSHFS.hs := SSH file system

[1]: https://github.com/m15k/google-drive-api/issues        "Google-Drive-API Library Issues"
[2]: https://github.com/m15k/google-drive-api               "Google-Drive-API Library"
[3]: http://www.haskell.org/cabal/users-guide/developing-packages.html#configurations "Cabal Configurations"
