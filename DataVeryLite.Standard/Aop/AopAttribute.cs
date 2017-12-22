#if NET40

using System;
using System.Runtime.Remoting.Proxies;

namespace DataVeryLite.Aop
{
    internal class AopAttribute : ProxyAttribute
    {
        public override MarshalByRefObject CreateInstance(Type serverType)
        {
            var realProxy = new AopRealProxy(serverType, base.CreateInstance(serverType));
            return realProxy.GetTransparentProxy() as MarshalByRefObject;
        }
    }
}

#endif
