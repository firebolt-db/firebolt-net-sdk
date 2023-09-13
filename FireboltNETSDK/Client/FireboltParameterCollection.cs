using System.Collections;
using System.Data;
using System.Data.Common;

namespace FireboltDotNetSdk.Client
{
    /// <summary>
    /// Represents a collection of parameters associated with a <see cref="FireboltCommand"/>. This class cannot be inherited.
    /// </summary>
    public sealed class FireboltParameterCollection : DbParameterCollection, IList<DbParameter>, IReadOnlyList<DbParameter>
    {
        private IList<DbParameter> _parameters = new List<DbParameter>();

        public FireboltParameterCollection()
        {
        }

        public FireboltParameterCollection(params DbParameter[] parameters)
        {
            foreach (DbParameter parameter in parameters)
            {
                _parameters.Add(parameter);
            }
        }


        /// <inheritdoc/>
        public override int Count => _parameters.Count;

        /// <inheritdoc/>
        public override object SyncRoot => ((ICollection)_parameters).SyncRoot;

        /// <inheritdoc/>
        public override int Add(object value)
        {
            DbParameter parameter = typeof(DbParameter).IsAssignableFrom(value.GetType()) ? (DbParameter)value : new FireboltParameter(FireboltParameter.defaultParameterName, value);
            Add(parameter);
            return _parameters.Count - 1;
        }

        public void Add(DbParameter parameter)
        {
            _parameters.Add(parameter);
        }

        /// <inheritdoc/>
        public override void Clear()
        {
            _parameters.Clear();
        }

        /// <inheritdoc/>
        public bool Contains(DbParameter parameter)
        {
            return Contains(parameter.ParameterName);
        }

        /// <inheritdoc/>
        public void CopyTo(DbParameter[] array, int arrayIndex)
        {
            _parameters.CopyTo(array, arrayIndex);
        }

        /// <inheritdoc/>
        public override bool Contains(object value)
        {
            if (typeof(DbParameter).IsAssignableFrom(value.GetType()))
            {
                return Contains((DbParameter)value);
            }
            foreach (DbParameter parameter in _parameters)
            {
                if (object.Equals(parameter.Value, value))
                {
                    return true;
                }
            }
            return false;
        }

        /// <inheritdoc/>
        public override int IndexOf(object value)
        {
            if (typeof(DbParameter).IsAssignableFrom(value.GetType()))
            {
                return IndexOf((DbParameter)value);
            }

            for (int i = 0; i < _parameters.Count(); i++)
            {
                if (_parameters[i].Value?.Equals(value) ?? false)
                {
                    return i;
                }
            }
            return -1;
        }

        public int IndexOf(DbParameter parameter)
        {
            return parameter.Value == null ? -1 : IndexOf(parameter.Value);
        }

        /// <inheritdoc/>
        public override void Insert(int index, object value)
        {
            Insert(index, new FireboltParameter(FireboltParameter.defaultParameterName, value));
        }
        public void Insert(int index, DbParameter parameter)
        {
            _parameters.Insert(index, parameter);
        }

        /// <inheritdoc/>
        public bool Remove(DbParameter parameter)
        {
            return _parameters.Remove(parameter);
        }

        /// <summary>
        /// Removes the parameter with the specified name from the collection.
        /// </summary>
        /// <param name="parameterName">The name of the parameter.</param>
        /// <returns><see langword="true"/> if the parameter was removed; <see langword="false"/> if a parameter with the specified name is not present in the collection.</returns>
        public bool Remove(string parameterName)
        {
            int index = IndexOf(parameterName);
            if (index < 0)
            {
                return false;
            }
            RemoveAt(index);
            return true;
        }

        /// <inheritdoc/>
        public override void Remove(object value)
        {
            if (value is FireboltParameter parameter)
            {
                Remove(parameter);
            }
            else if (value is string name)
            {
                Remove(name);
            }
        }

        /// <inheritdoc/>
        public override void RemoveAt(int index)
        {
            _parameters.RemoveAt(index);
        }

        /// <inheritdoc/>
        public override void RemoveAt(string parameterName)
        {
            Remove(parameterName);
        }

        /// <inheritdoc/>
        protected override void SetParameter(int index, DbParameter value)
        {
            _parameters[index] = value;
        }

        /// <inheritdoc/>
        protected override void SetParameter(string parameterName, DbParameter value)
        {
            int index = IndexOf(parameterName);
            if (index < 0)
            {
                _parameters.Add(value);
            }
            else
            {
                _parameters[index] = value;
            }
        }

        /// <inheritdoc/>
        public override int IndexOf(string parameterName)
        {
            for (int i = 0; i < _parameters.Count(); i++)
            {
                if (object.Equals(_parameters[i].ParameterName, parameterName))
                {
                    return i;
                }
            }
            return -1;
        }

        /// <inheritdoc/>
        public override bool Contains(string value)
        {
            foreach (DbParameter parameter in _parameters)
            {
                if (object.Equals(parameter.ParameterName, value))
                {
                    return true;
                }
            }
            return false;
        }

        /// <inheritdoc/>
        public override void CopyTo(Array array, int index)
        {
            _parameters.CopyTo((DbParameter[])array, index);
        }

        /// <inheritdoc/>
        public override IEnumerator<DbParameter> GetEnumerator()
        {
            return _parameters.GetEnumerator();
        }

        /// <inheritdoc/>
        protected override DbParameter GetParameter(int index)
        {
            return _parameters[index];
        }

        /// <inheritdoc/>
        protected override DbParameter GetParameter(string parameterName)
        {
            int index = IndexOf(parameterName);
            if (index < 0)
            {
                throw new ArgumentException($"Parameter \"{parameterName}\" not found.", nameof(parameterName));
            }
            return GetParameter(index);
        }

        /// <inheritdoc/>
        public override void AddRange(Array values)
        {
            foreach (var parameter in values.Cast<DbParameter>())
            {
                _parameters.Add(parameter);
            }
        }

        /// <inheritdoc/>
        public new FireboltParameter this[int index]
        {
            get => (FireboltParameter)base[index];
            set => base[index] = value;
        }

        /// <summary>Gets or sets the <see cref="FireboltParameter"/> with the specified name.</summary>
        /// <param name="parameterName">The name of the <see cref="FireboltParameter"/> in the collection.</param>
        /// <returns>The <see cref="FireboltParameter"/> with the specified name.</returns>
        public new FireboltParameter this[string parameterName]
        {
            get => (FireboltParameter)base[parameterName];
            set => base[parameterName] = value;
        }
    }
}
