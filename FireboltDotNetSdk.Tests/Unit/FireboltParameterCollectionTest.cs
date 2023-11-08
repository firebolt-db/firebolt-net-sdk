using FireboltDotNetSdk.Client;
using System.Data.Common;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    public class FireboltParameterCollectionTest
    {
        [Test]
        public void Empty()
        {
            DbParameterCollection collection = new FireboltParameterCollection();
            Assert.That(collection.Count, Is.EqualTo(0));
            object root = collection.SyncRoot;
            Assert.NotNull(root);
        }

        [Test]
        public void SeveralParameters()
        {
            DbParameterCollection collection = new FireboltParameterCollection(new FireboltParameter("one", 1), new FireboltParameter("pi", 3.14));
            Assert.That(collection.Count, Is.EqualTo(2));
            Assert.That(collection.Contains("one"), Is.EqualTo(true));
            Assert.That(collection.Contains("pi"), Is.EqualTo(true));
            Assert.That(collection.Contains("two"), Is.EqualTo(false));
            Assert.That(collection.Contains(1), Is.EqualTo(true));
            Assert.That(collection.Contains(3.14), Is.EqualTo(true));
            Assert.That(collection.Contains(2), Is.EqualTo(false));
            Assert.That(collection.Contains(new FireboltParameter("one", 1)), Is.EqualTo(true));
            Assert.That(collection.Contains(new FireboltParameter("pi", 3.14)), Is.EqualTo(true));
            Assert.That(collection.Contains(new FireboltParameter("two", 2)), Is.EqualTo(false));
            Assert.That(collection.IndexOf(1), Is.EqualTo(0));
            Assert.That(collection.IndexOf(3.14), Is.EqualTo(1));
            Assert.That(collection.IndexOf(2), Is.EqualTo(-1));
            Assert.That(collection.IndexOf("one"), Is.EqualTo(0));
            Assert.That(collection.IndexOf("pi"), Is.EqualTo(1));
            Assert.That(collection.IndexOf("does-not-exist"), Is.EqualTo(-1));

            Assert.That(collection.IndexOf(new FireboltParameter("one", 1)), Is.EqualTo(0));
            AssertParameterNames(collection, new List<string>() { "one", "pi" });

            Assert.That(collection.Contains("e"), Is.EqualTo(false));
            collection.Add(new FireboltParameter("e", 2.7));
            Assert.That(collection.Contains("e"), Is.EqualTo(true));
            AssertParameterNames(collection, new List<string>() { "one", "pi", "e" });

            collection.Add(2);
            AssertParameterNames(collection, new List<string>() { "one", "pi", "e", FireboltParameter.defaultParameterName });
            AssertParameterValues(collection, new List<object?>() { 1, 3.14, 2.7, 2 });

            collection.Clear();
            Assert.That(collection.Count, Is.EqualTo(0));
        }

        [Test]
        public void AddRange()
        {
            DbParameterCollection collection = new FireboltParameterCollection();
            collection.AddRange(new DbParameter[0]);
            Assert.That(collection.Count, Is.EqualTo(0));

            collection.AddRange(new DbParameter[] { new FireboltParameter("one", 1), new FireboltParameter("two", 2) });
            AssertParameterNames(collection, new List<string>() { "one", "two" });
            AssertParameterValues(collection, new List<object?>() { 1, 2 });

            collection.AddRange(new DbParameter[] { new FireboltParameter("e", 2.7), new FireboltParameter("pi", 3.14) });
            AssertParameterNames(collection, new List<string>() { "one", "two", "e", "pi" });
            AssertParameterValues(collection, new List<object?>() { 1, 2, 2.7, 3.14 });
        }

        [Test]
        public void AsList()
        {
            DbParameterCollection collection = new FireboltParameterCollection();
            collection["two"] = new FireboltParameter("two", 2);
            AssertParameterNames(collection, new List<string>() { "two" });
            AssertParameterValues(collection, new List<object?>() { 2 });
            collection["two"].Value = 22;
            AssertParameterValues(collection, new List<object?>() { 22 });
            collection[0].Value = 11;
            collection[0].ParameterName = "one";
            AssertParameterNames(collection, new List<string>() { "one" });
            AssertParameterValues(collection, new List<object?>() { 11 });
        }

        [Test]
        public void ChangeParameters()
        {
            DbParameterCollection collection = new FireboltParameterCollection(new FireboltParameter("one", 1), new FireboltParameter("pi", 3.14));
            Assert.That(collection.Count, Is.EqualTo(2));
            AssertParameterNames(collection, new List<string>() { "one", "pi" });
            AssertParameterValues(collection, new List<object?>() { 1, 3.14 });
            collection.Insert(1, 2);
            AssertParameterNames(collection, new List<string>() { "one", FireboltParameter.defaultParameterName, "pi" });
            AssertParameterValues(collection, new List<object?>() { 1, 2, 3.14 });
            collection.RemoveAt(2);
            AssertParameterValues(collection, new List<object?>() { 1, 2 });
            collection.Remove(3); // should not work because there is no parameter with value=3
            AssertParameterValues(collection, new List<object?>() { 1, 2 });
            collection.Remove("one");
            AssertParameterValues(collection, new List<object?>() { 2 });
            collection.RemoveAt(FireboltParameter.defaultParameterName);
            Assert.That(collection.Count, Is.EqualTo(0));
        }

        [Test]
        public void RemoveParameter()
        {
            FireboltParameter one = new FireboltParameter("one", 1);
            FireboltParameter pi = new FireboltParameter("pi", 3.14);
            DbParameterCollection collection = new FireboltParameterCollection(one, pi);
            collection.Remove(one);
            AssertParameterNames(collection, new List<string>() { "pi" });
            AssertParameterValues(collection, new List<object?>() { 3.14 });

            collection.Remove("e"); // this entry does not exist
            Assert.That(collection.Count, Is.EqualTo(1));
            collection.Remove("pi");
            Assert.That(collection.Count, Is.EqualTo(0));
        }

        [Test]
        public void CopyTo()
        {
            FireboltParameter[] parameters = new FireboltParameter[] { new FireboltParameter("one", 1), new FireboltParameter("two", 2) };
            DbParameterCollection collection = new FireboltParameterCollection(parameters);
            FireboltParameter[] parametersCopy = new FireboltParameter[2];
            collection.CopyTo(parametersCopy, 0);
            Assert.That(parameters, Is.EqualTo(parametersCopy));
        }

        private void AssertParameterNames(DbParameterCollection collection, List<string> expectedNames)
        {
            List<string> names = new();
            foreach (DbParameter p in collection)
            {
                names.Add(p.ParameterName);
            }
            Assert.That(names, Is.EqualTo(expectedNames));
        }

        private void AssertParameterValues(DbParameterCollection collection, List<object?> expectedValues)
        {
            List<object?> values = new();
            foreach (DbParameter p in collection)
            {
                values.Add(p.Value);
            }
            Assert.That(values, Is.EqualTo(expectedValues));
        }
    }
}