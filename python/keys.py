"""Тестовые ключи аккаунтов для нагрузчика (N02).

ВНИМАНИЕ: это ТЕСТОВЫЕ ключи дев-стенда, закоммичены по решению владельца
(см. docs/edits/feature/python-loadgen/spec.md §5). Не используйте их в
продуктивной среде. Переопределить набор можно через окружение:
`LOADGEN_KEYS` — JSON-массив объектов {"sk": ..., "pk": ...}."""


import json
import os

# (sk_base58, pk_base58)
_DEFAULT_KEYS = [
    ("4wD1ABQ1BdvLYNXo6tjvMNF1mf8zqkHqy9mXoozCcdJz", "6s2kd6cwHR9NCWVUghTw83wRBsaTBYAzFCWcVmAcgXto"),
    ("FK9HhUwjtrpRjS2enKTaYkpk7ojSxUM43fMduwQSgUyZ", "NQ4rVyGZtyb5misEvWEHaR6wjqJsF8vWNUanSbTJPz4"),
    ("DJEJaeoBBmmQcmneQj5vNXPYTRN5TE5oC5zcABxE47Zw", "ADYNeuoVmSkNnjfXAPSYGCcJSwXtpTmE4DysXVzpUae5"),
    ("DXJLZqexghewPjy1accVFzuyctY6ee7vzvq2ZvM9vJNe", "GKVCsG2kghsLB46Nt2TT3dyTcvf6VzwDRiUa1r7GZMBt"),
    ("4PG43knssVqgACrV6A8C5665orn9i8NgYocNuy8JAdg8", "9x6mELceJFQYr4GbfJ98EMDLWVk2xMLQ2yftLHZLfTyi"),
    ("FmgkVFB7X9HXWLCVytYUoUSvsUV9amTaoJfTWQJKjUmE", "DW2owfqizS13SaDR39HPQT3psck6F9cU5UVdJrDpL7TH"),
    ("FgGBopQT4aEexe17tqgx6ikVcBdnLMogK28nRawc8EGx", "53xCyvYTAtsAusLgWc1FJtVpnp46JUHdPMNET7CiZA6d"),
    ("Xj7961F4pS9dNKowFAUZCtGb2jQtRsyXjZ4sqtJseCG", "7j6rNZyjVQbtfAH95R8KqWSiezQXs6sBWn7pkvbeXmMH"),
    ("9d957YXkDTX64ufhmxeD3h1bjmjJqWdpy19BNs25cVE9", "EVB51EYYwv4cXaR7BuXBL35LyG9BcxHbHZDDZPm6vS19"),
    ("7Dv7jd5LJzSU7G4BrVF35ydo6HGeDr6Sro1XKs5PjsJG", "841bWoLH4hzH3gEPvdwciGBrCr1qkrorQbXc5xce6xuc"),
    ("9LdKsSYR3riUnSWMjrBvSQ83dBMurV2xKW1K8RwCVMYG", "C2NY9LA9U45p2vW5GJzPBBRT3MiwokaBcY1SpNekhWC6"),
    ("9xnXhtK5BZNDR8wUdaNz6dt4ngQUiWvi3GDbKyxS9Am5", "CggwrX5Gd61spXauPuoLRFixz3FAn5f5eP69XBPJzCaZ"),
    ("GcWhpTH5S1rh23bj7WsRodc2pbnjBed1NkuXDTcL1iQ6", "3VCdwYJPdgJeKPAr7fqrVb5gXEX5Phy6p9zLEpsWVVC1"),
    ("ATm3JsBRx5eAaesKzSyns8gmqv6DUACaj9eDGGBFjyXw", "FkUYHRCSEmT7ejD9CprDpgtN66Pmqe9bEWcQR8zunfxv"),
    ("EPhfhqL4PemJPwTS6hQUkBPg6qDv28Exotb83JbqdRpg", "4ACFJ6MWVuPfG4uKJ7NP3yC6mfHXY3mnrtqrCC71u453"),
    ("4yApLaaxwkDTRzREABVM2uE9Q5ibUhkc1ov1LV3EEVTE", "FvgFLHGLGmtBvUvg6UKFNBNiqSxmuSqjvp2ZbReebqew"),
    ("FHdNtd8agsxkeYDeJsY8ZU8Cbq6nrjdfDfg43AELDHTU", "GAyReNvJJvTM8tpa9cnWZwGRfjRtFbzoopRSvettsEou"),
    ("Dah4mkaiQz8UAbNENtaEzJwpufpe4eGLzzEqLGfUkmhi", "CgUVE4UJWZMUhggzMrhNkkaaPuF5DVTarJpytns97DNZ"),
    ("9NCZd9rSwAHSKcWiT4v5YPF3vvVahwahuXJGva9qFcCK", "Fuwhrb3RHaobBK49qYfZyDsyj4GmvXRiBdLh7TmvpWAJ"),
    ("FTKPqKisDSbpNeeBo3qzFWPBAaVWD5fhmEqocHtTS8TX", "Az8fBfQL8oUDMUJHGt5WWdyYLEoiKp6NTCsWBxuw2fSf"),
    ("6babv1tPq47Qx2ZS1gL4ShisyHpsA7mxTf85kEg2qize", "2BbvTBGywLGhcX8Gcshxq7JiPxm2eWp1gpEXjvFqtKHF"),
    ("BU3E2ZMs5qjsig3hbFScegKdfH5Misvn4hiE2BTZ6Wwi", "FdTgeFk1odDNJD44eVtSqbeukXWtWpubDSz9UBNup6bp"),
    ("FGnghsJCBCnjkawep7793JGK8pKz4dG4yLdcvfgFcVR", "FBNtfj2RD9NdjtARStFZe6CHLwPAZyYc6XQHhsJaiMwT"),
    ("7Q9QtFvzypEjAeKjZzpYSYRXTFsnkX8sEaqjLXhQim9P", "Fpq1ZJnQJHuaJv9voECVKgotGmWjr6rGqMosRDMFun8r"),
    ("2S2YgwVrvr7vpCX4tGsHZtdSo7WpwMfpmxUsGThSMJxe", "6adYWeXvfZidxYT6b27HnWyTaaM4sZGicxiwCoMZD2Rw"),
    ("4Dy7qvadNEx76CBf4mYkFQ6857VJRM6tZJ42wdYC8Q47", "BVkZuVshL5ba2tVroNEw6NnQRJcjoZ6bgR4CwxCYhhtt"),
    ("8LTiLT68Zv9nKBJVL4v8zpcj2qHQZCF91oggxz57JoFw", "Bn8LngZBBbDKBfZAZEjxJFUbAfidWn46aaQ7haDdcCya"),
    ("DsCnwtPgfVbVG3k9wkZZwxMNYqHqpYAz94X3CAocbsw5", "HTuYBS6UaV3DLyAk19v8e2XD2y5zzigvw6LYLzvZMLbz"),
    ("9eWvrZWE1vniYgiLQD7NAhUVNDqFWt2fUMjwNpYZz45t", "9oTsawHd2YY2JdyJCATbNn8gfagEHxZD5NQUnp8q6cFx"),
    ("CmEYPd7qXeB2tniB3fYN7ztV7LBDPMqiVDDwp9vy1bbZ", "FcpiXnBW8tjhoNeHk4Cg2ZPn4Lxq86UH6iUQJK4Btexn"),
    ("8DvLc3pg9RmoMRnsjDZiZGKYerMjd6bqKdnhEYarWgfy", "9FiffPovpNutEAWFDmAxrgyTzG5L4SJzwPyhNniWMAQd"),
    ("HxPWzwDuyRmXiksqpnmEcAX32FpxdNftKGhNmLZA4Pip", "7M6D7xompMcaQZmnQZSuqCLi7NR4Yr6qDNHfeYQYjrBM"),
]


def load_keys():
    """Возвращает список (sk, pk): из `LOADGEN_KEYS`, иначе тестовый набор."""
    raw = os.environ.get("LOADGEN_KEYS")
    if raw:
        parsed = json.loads(raw)
        return [(item["sk"], item["pk"]) for item in parsed]
    return list(_DEFAULT_KEYS)
