# Contributing to Transfer

The goal of this document is to resolve uncertainties that arise when writing new or refactoring old code, and during pull request reviews. The best practices apply in the *general case*. This means that a particular piece of code or a particular component can be implemented differently than it is prescribed by this document. However, a sound reason is required for each such deviation.

## Getting started

1. Read a [readme.md](./README.md)
1. Make yourself familiar with common terms in [glossary.md](./GLOSSARY.md)

## Guidelines

Over years of writing big scalable systems, we are convinced that striving for simplicity wherever possible is the only way to build robust systems.
This simplicity could be in design, could be in coding, or could be achieved by rewriting an entire module, that you may have painstakingly finished yesterday.
Simple code is always preferable to smart code.

We use feature branches, PR should contain ticket in a title, we accept typo-s fixes without ticket but any meaningful change must have some ticket assigned to it.
Before contributing large or high impact changes, make the effort to coordinate with the maintainers of the module/project before submitting a pull request. This prevents you from doing extra work that may or may not be merged.

Large PRs that are just submitted without any prior communication are unlikely to be successful.

Larger changes typically work best with design documents. These are focused on providing context to the design at the time the feature was conceived and can inform future documentation contributions. Design doc could be introduced as part of PR-Description, or if feature can't fit into one PR-s we can commit them as separate step in [RFC-like](https://a.yandex-team.ru/review/3559155/details) format.
See examples:
1. Big new feature: [CH Local Transformer PR](https://a.yandex-team.ru/review/3460298/details)
1. Big refactoring: [Refactoring of tasks](https://a.yandex-team.ru/review/3518442/details)
1. Small but impactful change: [Temporal types nil handling in pg](https://a.yandex-team.ru/review/3543565/details)

### Code Reviews

If your pull request (PR) is opened, it should be assigned to reviewers within one of **Feature Owners** and someone **else**. Normally each PR requires at least **2 Ships** from assigned reviewers. Those reviewers will do a thorough code review, looking at correctness, bugs, opportunities for improvement, documentation, comments and style.

#### PR organization philosophy

A pull request (PR) is a package of work that engages three parties:

1. the engineer delivering the change
1. the reviewer(s) who analyze and review it before it is integrated (merged) to the project
1. later maintainers of the code who need to understand and modify the code further

It is important to understand that a PR is not just about changing the code. It is really about creating a dialogue, via the history of commits, that helps both reviewers and future maintainers understanding the concepts and the components inside the code.
In a nutshell, the programmer’s job is to organize the PR to be easily **reviewable** and to make the code more easily **maintainable**. To achieve **reviewability**, the best way is to make **each review cover one conceptual unit of work towards the goal, and explain intent (in review description) separately from the code**.  To achieve **maintainability**, the programmer should explain their **choices** and spell out (in review description) what they left out, when applicable.


PR title must be readable. We always refer to ticket in PR-title (like `TM-{NUBMER}: My Awesome Change`). There is a plenty articles about good PR description, like [here](https://gist.github.com/robertpainsi/b632364184e70900af4ab688decf6f53#rules-for-a-great-git-commit-message-style) or [here](https://www.freecodecamp.org/news/git-best-practices-commits-and-code-reviews/), use them as a framework for your commit message.

#### Best practice

As author or reviewer of PR try to follow reviews best practice, for example [here](https://www.atlassian.com/blog/add-ons/code-review-best-practices) or [here](https://docs.gitlab.com/ee/development/code_review.html#best-practices)

Here is short summary of such practice:

1. Be kind.
1. Accept that many programming decisions are opinions. Discuss tradeoffs, which you prefer, and reach a resolution quickly.
1. Ask questions; don’t make demands. (“What do you think about naming this :user_id?”)
1. Ask for clarification. (“I didn’t understand. Can you clarify?”)
1. Avoid selective ownership of code. (“mine”, “not mine”, “yours”)
1. Avoid using terms that could be seen as referring to personal traits. (“dumb”, “stupid”). Assume everyone is intelligent and well-meaning.
1. Be explicit. Remember people don’t always understand your intentions online.
1. Be humble. (“I’m not sure - let’s look it up.”)
1. Don’t use hyperbole. (“always”, “never”, “endlessly”, “nothing”)
1. Be careful about the use of sarcasm. Everything we do is public; what seems like good-natured ribbing to you and a long-time colleague might come off as mean and unwelcoming to a person new to the project.
1. Consider one-on-one chats or video calls if there are too many “I didn’t understand” or “Alternative solution:” comments. Post a follow-up comment summarizing one-on-one discussion.
1. If you ask a question to a specific person, always start the comment by mentioning them; this ensures they see it if their notification level is set to “mentioned” and other people understand they don’t have to respond.

Also consider follow guidelines when working on PR:

1. It’s a good idea for the person who started a issue or discussion to resolve it when there’s no further action to take.
1. Use reactions in response to someone else’s comments as a shortcut to show we’ve read them and understand or agree (or not).
1. Don’t review more than 200-400 lines of code at a time. As author minimise PR size as much as possible, there is a [study](https://static1.smartbear.co/support/media/resources/cc/book/code-review-cisco-case-study.pdf) that defined optimal PR size is <400 LoC, try to stick with this border if possible. As reviewer take small breaks between review session.
1. Use memes to illustrate your points in review if that possible, it's always helpful to have such friendly and funny part inside of boiled PR discussion.


#### Moving blocks of code

Changes may require moving blocks of code (or tests) to new packages or files, and then making edits to the moved code.
We recommend splitting this type of refactoring into two commits.
The first PR relocates the code without logical changes. The second makes the required logical changes to the code in its new location.
This makes it easier for reviewers to scrutinize the moved code and logical changes separately.
If both are contained in a single commit the logical changes will be “hidden” from the reviewer in a large block of newly added lines.

#### Aesthetic changes

Sometimes an author feels compelled to update spelling, grammar, or the overall structure of the code for legibility or other subjective purposes. When doing so, the author should be mindful that moving/editing code for aesthetic reasons also has a downside: it pollutes the output of arc blame, which connects code to its last author/maintainer.

It's nice for a blame to be as clean as possible. It's nice for the history of a code line to have fewer entries. Being mindful of blame is just common hygiene. There is a bar for changing a line of code; we don't do it gratuitously. For example, a one-line change to fix a typo in a comment impairs the output of blame and also has an opportunity cost in your use of time.

Besides the effect of blame churn on humans, it also has an effect on machines. For example, arcanum might start suggesting this person as a reviewer for future patches.

Authors should thus carefully weigh the legibility gains of aesthetic changes with the maintainability cost of impairing history navigation.

What is aesthetic changes that we should minimise:

1. Resorting methods in file
1. Resorting file structure
1. Rewriting piece of code for no reason (i.e. no future plan to use it or clear refactoring objections)

## Code style

Keeping a consistent style for code, code comments, commit messages, and pull requests is very important for a project with big history like **transfer**.

#### We highly recommend you refer to and comply to the following style guides when you put together your pull requests:

1. We're following [Go Code Review](https://github.com/golang/go/wiki/CodeReviewComments) and [Effective Go](https://go.dev/doc/effective_go)
1. Use `ya tool yoimports -w` to format your code before committing.

#### In addition to the documents above, we have following rules, which are more strict:

1. [Naked returns](https://github.com/golang/go/wiki/CodeReviewComments#naked-returns) are prohibited in functions that return value. [Named result parameters](https://github.com/golang/go/wiki/CodeReviewComments#named-result-parameters) are allowed, but return statement should be filled.

1. If a struct implements an interface, we make it explicit and ensure compile-time verification with a line like this:

   `var _ OurInterface = (*OurStruct)(nil)`

   This statement will fail to compile if `*OurStruct` stops mathcing `OurInterface`

#### While making your PR take in consideration follow principles:

1. If you see any code which clearly violates the style guide, please fix it and send a **separate** pull request with just it. No need to fix it **within** feature branch. We don't encourage [opportunistic refactorings](https://softwareengineering.stackexchange.com/questions/244807/reconciling-the-boy-scout-rule-and-opportunistic-refactoring-with-code-reviews).
1. **Consistency** is more important than beauty of code in a somewhat large and long lived project, it is always a good idea to check whether there are similar term or concepts in the code.


### Code Comments

First of all - let the code speak for itself, but in many cases it's not enough, so we may need some comments.

#### Why does a good comment matter?

1. To speed up the reviewing process
1. To help maintain the code
1. To improve the API document readability
1. To improve the development efficiency of the whole team


#### Where/When to comment?

1. For important code
1. For obscure code
1. For tricky or interesting code
1. For a complex code block
1. If a bug exists in the code, but you cannot fix it, or you just want to ignore it for the moment
1. If the code is not optimal, but you don't have a smarter way now
1. To remind yourself or others of missing functionality or upcoming requirements not present in the code

#### Tips for a good comment:

1. Comment code while writing it
1. Do not assume the code is self-evident
1. Avoid unnecessary comments for trivial code
1. Write comments that would help you, if you were the one to see this code for the first time
1. Make sure the comment is up-to-date

### Error concepts

Here are some of the important things that we care about:

1. Errors should not cause running applications (e.g. a dataplane node) to terminate without handling it. Customers would consider this an unacceptable defect. **Correct and deliberate error handling is a core part of product quality and stability**.
1. Users **will** read the text of error messages, however users cannot be assumed to understand the source code. If an error message is confusing, the users will ask confusing questions to our support. If an error message is misguiding, the users will ask the wrong questions to our support. And so on. **Error messages should be clear and accurate and avoid referring to source code internals**.
1. Any error visible to one user will likely be visible to dozens, if not hundreds of users eventually. We want our users to **understand** what they should do about an error on their own, so they do not need to reach out to support. For this, we want our error messages to be **self-explanatory** and include **hint** annotations. We also make specific  **error codes** (e.g. Termination reasons) part of our public, documented API.
1. Errors are part of the **API** and thus error situations should be covered in **unit tests**.
1. Error make their way to log files and crash reports and can contain **user**-provided data. We care to separate customer confidential data from non-confidential data in log files and crash reports, and so we need to distinguish **sensitive** data inside **error** objects too.

### Organization of code into packages

We use Go packages to delimit units of functionality.

We generally like **smaller** packages with a **tight** scope, but not **so small** that components that are always updated together are pulled away from each other by package boundaries.

### When to create new packages

Here are example reasons to create new packages:

1. New functionality is being added that is self-contained and can be described independently.
1. An existing package has grown too large. For example:
   1. it takes too long to run its unit tests.
   1. there are so many components that newcomers become confused; it is generally harder to learn.
   1. making changes to one place require verification to many other places, because of insufficient encapsulation.
   1. another team has expressed interest in reusing a sub-part of the package but they cannot import the entire package.
1. A portion of code uses custom build rules.
1. A piece of code is identified as a valuable standalone library that can be reused in other projects.

## Contribution Guideline Evolution

We should expect that rules in `CONTRIBUTING.md` changes as rare as possible, so adding a new rule must follow this requirements:

1. Change is accepted by [**maintainers**](https://a.yandex-team.ru/arcadia/groups/data-transfer), so driving by all of contributors

It's also nice to have for all new conventions follows this principle:

1. Change is accepted by **broader community**, so exist at least in some other [big opensource projects](https://evanli.github.io/Github-Ranking/Top100/Go.html)
1. Change is **cheap** to maintain, so we have a simple linter
1. Change is **easy** to enforce in exist codebase, or it's already there.

Every new rule applies only for new code and followed in PR discussion and/or automation check. Don't expect or force rewriting or refactoring old code base for new rule.

# Notice to external contributors


## General info

Hello! In order for us (YANDEX LLC) to accept patches and other contributions from you, you will have to adopt our Yandex Contributor License Agreement (the “**CLA**”). The current version of the CLA can be found here:
1) https://yandex.ru/legal/cla/?lang=en (in English) and
2) https://yandex.ru/legal/cla/?lang=ru (in Russian).

By adopting the CLA, you state the following:

* You obviously wish and are willingly licensing your contributions to us for our open source projects under the terms of the CLA,
* You have read the terms and conditions of the CLA and agree with them in full,
* You are legally able to provide and license your contributions as stated,
* We may use your contributions for our open source projects and for any other of our projects too,
* We rely on your assurances concerning the rights of third parties in relation to your contributions.

If you agree with these principles, please read and adopt our CLA. By providing us your contributions, you hereby declare that you have already read and adopt our CLA, and we may freely merge your contributions with our corresponding open source project and use it in further in accordance with terms and conditions of the CLA.

## Provide contributions

If you have already adopted terms and conditions of the CLA, you are able to provide your contributions. When you submit your pull request, please add the following information into it:

```
I hereby agree to the terms of the CLA available at: [link].
```

Replace the bracketed text as follows:
* [link] is the link to the current version of the CLA: https://yandex.ru/legal/cla/?lang=en (in English) or https://yandex.ru/legal/cla/?lang=ru (in Russian).

It is enough to provide us such notification once.

## Other questions

If you have any questions, please mail us at opensource-support@yandex-team.ru.
