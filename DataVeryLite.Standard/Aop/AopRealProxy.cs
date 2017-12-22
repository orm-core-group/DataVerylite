#if NET40

using System;
using System.Reflection;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Activation;
using System.Runtime.Remoting.Messaging;
using System.Runtime.Remoting.Proxies;
using System.Runtime.Remoting.Services;

namespace DataVeryLite.Aop
{
    internal class AopRealProxy : RealProxy
    {
        private readonly Type _currentType;

        private readonly MarshalByRefObject _target = null;

        public AopRealProxy(Type serverType,MarshalByRefObject target)
            : base(serverType)
        {
            _currentType = serverType;
            _target = target;
        }
        public override IMessage Invoke(IMessage msg)
        {
            if (msg is IConstructionCallMessage) 
            {
                var constructCallMsg = msg as IConstructionCallMessage;
                RealProxy defaultProxy = RemotingServices.GetRealProxy(_target);
                defaultProxy.InitializeServerObject(constructCallMsg);
                return EnterpriseServicesHelper.CreateConstructionReturnMessage(constructCallMsg, (MarshalByRefObject)GetTransparentProxy());
            }

            if (msg is IMethodCallMessage)
            {
                var callMsg = msg as IMethodCallMessage;
                object[] args = callMsg.Args;
                IMessage message;
                try
                {
                    if (callMsg.MethodName.StartsWith("set_") && args.Length == 1)
                    {
                        _currentType.InvokeMember("InvokeSetProperty", BindingFlags.InvokeMethod, null, _target, new[] { callMsg.MethodName.Replace("set_", ""), args[0] });
                    }
                    else  if (callMsg.MethodName.StartsWith("get_") && args.Length == 0)
                    {
                        _currentType.InvokeMember("InvokeGetProperty", BindingFlags.InvokeMethod, null, _target, new object[] { callMsg.MethodName.Replace("get_", "") });
                    }
                    else
                    {
                        _currentType.InvokeMember("InvokeMethod", BindingFlags.InvokeMethod, null, _target, new object[] { callMsg.MethodName, args });
                    }

                    return RemotingServices.ExecuteMessage(_target, callMsg);
                }
                catch (Exception e)
                {
                    message = new ReturnMessage(e, callMsg);
                }
                return message;
            }

            return msg;
        }
    }
}

#endif