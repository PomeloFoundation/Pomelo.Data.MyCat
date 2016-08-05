# Pomelo.Data.MyCat

[![Travis build status](https://img.shields.io/travis/PomeloFoundation/Pomelo.Data.MyCat.svg?label=travis-ci&branch=master&style=flat-square)](https://travis-ci.org/PomeloFoundation/Pomelo.Data.MyCat)
[![AppVeyor build status](https://img.shields.io/appveyor/ci/Kagamine/Pomelo-Data-MyCat/master.svg?label=appveyor&style=flat-square)](https://ci.appveyor.com/project/Kagamine/pomelo-data-mycat/branch/master) [![NuGet](https://img.shields.io/nuget/v/Pomelo.Data.MyCat.svg?style=flat-square&label=nuget)](https://www.nuget.org/packages/Pomelo.Data.MyCat/) [![Join the chat at https://gitter.im/PomeloFoundation/Home](https://badges.gitter.im/PomeloFoundation/Home.svg)](https://gitter.im/PomeloFoundation/Home?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Contains MyCat (An open source MySQL cluster proxy which based on Cobar) implementations of the System.Data.Common(Both .NET Core and .NET Framework) interfaces.

## Getting Started

To add the `Pomelo.Data.MyCat` into your `project.json`:

```json
{
  "version": "1.0.0-*",
  "buildOptions": {
    "emitEntryPoint": true
  },

  "dependencies": {
    "Microsoft.NETCore.App": {
      "type": "platform",
      "version": "1.0.0"
    },
    "Pomelo.Data.MyCat": "1.0.0"
  },

  "frameworks": {
    "netcoreapp1.0": {
      "imports": "dnxcore50"
    }
  }
}
```

`MyCatConnection`, `MyCatCommand` and etc were included in the namespace `Pomelo.Data.MyCat`. The following console application sample will show you how to use this library to write a record into MyCat database.

```C#
using Pomelo.Data.MyCat;

namespace MyCatAdoSample
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var conn = new MyCatConnection("server=localhost;database=adosample;uid=root;pwd=yourpwd"))
            {
                conn.Open();
                using (var cmd = new MyCatCommand("INSERT INTO `test` (`content`) VALUES ('Hello MyCat')", conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }
    }
}
```

## Contribute

One of the easiest ways to contribute is to participate in discussions and discuss issues. You can also contribute by submitting pull requests with code changes.

## License

[MIT](https://github.com/PomeloFoundation/Pomelo.Data.MyCat/blob/master/LICENSE)
