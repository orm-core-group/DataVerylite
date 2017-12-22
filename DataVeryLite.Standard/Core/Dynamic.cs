using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;

namespace DataVeryLite.Core
{
    public class Dynamic : DynamicObject
    {
        private readonly Dictionary<string,object> _properties=new Dictionary<string, object>(); 

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {

            var name = binder.Name;

            return _properties.TryGetValue(name, out result);

        }

        public override bool TrySetMember(SetMemberBinder binder, object value)
        {

            var name = binder.Name;

            _properties[name] = value;

            return true;

        }

        public object GetProperty(string name)
        {
            object value;
            if (_properties.TryGetValue(name, out value))
            {
                return value;
            }
            else
            {
                return null;
            }
        }

        public void SetProperty(string name, object value)
        {
            _properties[name] = value;
        }
    }
}
