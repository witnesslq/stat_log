#Developer Guide - Contents
* [1.Introduction and Getting Started](#1)
* [2.Resources](#2)
* [3.How TinyTemplate Works](#3)
* [3.1The Fundamental Pattern](#3.1)
#1.Introduction and Getting Started
[TinyTemplate](http://git.oschina.net/tinyframework/tiny) is a Java-based template engine, a simple and powerful development tool that allows you to easily create and render documents that format and present your data.In this guide,we hope to give an overview of the basics of development using 
TinyTemplate.
##Building Web Applications with TinyTemplate
##Downloading TinyTemplate
You can download the latest release version of [TinyTemplate](http://git.oschina.net/tinyframework/tiny)
#Template + data-model = output
Let's assume that you need a html page to render on website,similar to this:

**output**
```html
<html>
<head>
  <title>Welcome!</title>
</head>
<body>
  <h1>Welcome Young!</h1>
  <p>Our latest product:
  <a href="products/tiny.html">tinygroup</a>!
</body>

</html>
```
But the user name ("Young" above) should depend on who the logged in Web page visitor is, and the latest product should come from a database and thus it potentially changes too. Thus you can't enter these into the HTML directly, you can't use static HTML. Instead, you can use a template of the desired output. The template is the same as the static HTML would be, except that it contains some instructions to TinyTemplate that makes it dynamic:

**template**
```html
<html>
<head>
  <title>Welcome!</title>
</head>
<body>
  <h1>Welcome ${user}!</h1>
  <p>Our latest product:
  <a href="${latestProduct.url}">${latestProduct.name}</a>!
</body>
</html>
```
The template is stored on the Web server, usually just like the static HTML page would be. But whenever someone visits this page, TinyTemplate will step in and transform the template on-the-fly to plain HTML by replacing the ${...}-s with up-to-date content, and send the result to the visitor's Web browser. So the visitor's Web browser will receive something like the first example HTML (i.e., plain HTML without TinyTemplate instructions), and it will not perceive that TinyTemplate is used on the server. (Of course, the template file stored on the Web server is not changed by this; the substitutions only appear in the Web server's response.)

Note that the template doesn't contain the programming logic to find out who the current visitor is, or to query the database to get the latest product. The data to be displayed is prepared outside TinyTemplate, usually by parts written in some "real" programming language like Java. The template author needn't know how these values were calculated. In fact, the way these values are calculated can be completely changed while the templates can remain exactly the same, and also, the look of the page can be completely changed without touching anything but the template. This separation of presentation logic and business logic can be especially useful when the template authors (designers) and the programmers are different individuals, but also helps managing application complexity if they are the same person. Keeping templates focused on presentation issues (visual design, layout and formatting) is a key for using template engines like TinyTemplate efficiently.
#How TinyTemplate Works
'The Fundamental Pattern'

When using TinyTemplate in an application program or in a servlet (or anywhere, actually), you will generally do the following: 
1. Initialize TinyTemplateServlet. This applies to both usage patterns for Velocity, the Singleton as well as the 'separate runtime instance' (see more on this below), and you only do this once.
2. Create a Context object (more on what that is later).
3. Add your data objects to the Context.
4. Choose a template.
5. Merge the template and your data to produce the ouput.

#Using TinyTemplate
If you are using TinyTemplateServlet or other web frameworks,you may never call TinyTemplate directly.However, if you use TinyTemplate for non-web purposes, or create your own web framework you will need to directly call the TemplateEngine  similar to the fundamental pattern shown earlier. One important additional thing to remember is to initialize the TinyTemplate Engine before using it to merge templates.  